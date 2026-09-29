//! Runs the reserve and release scripts against Redis (`docker compose up -d
//! redis`), in database 15 so the tests never see, or are slowed down by, the
//! data in the database the API uses. Override with `TEST_REDIS_URL`. Each test
//! uses its own random event id.

use chrono::Utc;
use redis::AsyncCommands;

use conference::cache::{self, JoinOutcome, ReleaseOutcome, ReserveOutcome, RedisConn};

const HOLD_MS: u64 = 60_000;
const MAX_SEATS: u32 = 4;

async fn connect() -> RedisConn {
    dotenvy::dotenv().ok();
    let url = std::env::var("TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".into());
    let client = redis::Client::open(url).unwrap();
    redis::aio::ConnectionManager::new(client).await.expect("Redis should be running for these tests")
}

/// A 10-seat event, and a helper that admits a user (an ACTIVE token)
async fn event(con: &mut RedisConn) -> i32 {
    let event_id = 1_000_000 + (uuid::Uuid::new_v4().as_u128() % 1_000_000_000) as i32;
    cache::init_event(con, event_id, 10, Utc::now()).await.unwrap();
    event_id
}

async fn admit(con: &mut RedisConn, event_id: i32, user_id: i32) -> String {
    let token_id = uuid::Uuid::new_v4().to_string();
    let key = cache::token_key(&token_id);
    let _: () = redis::pipe()
        .hset_multiple(&key, &[("user", user_id.to_string()), ("event", event_id.to_string()), ("status", "ACTIVE".into())])
        .pexpire(&key, 600_000)
        .set(format!("user_token:{event_id}:{user_id}"), &token_id)
        .zadd(format!("active_tokens:{event_id}"), &token_id, Utc::now().timestamp_millis() + 600_000)
        .query_async(con)
        .await
        .unwrap();
    token_id
}

async fn reserve(con: &mut RedisConn, event_id: i32, user_id: i32, seats: &[u32]) -> ReserveOutcome {
    let resv = uuid::Uuid::new_v4().to_string();
    cache::reserve_seats(con, event_id, user_id, &resv, seats, HOLD_MS, MAX_SEATS).await.unwrap()
}

async fn get_i64(con: &mut RedisConn, key: String) -> i64 {
    con.get::<_, Option<i64>>(key).await.unwrap().unwrap_or(0)
}

async fn cleanup(con: &mut RedisConn, event_id: i32) {
    let keys: Vec<String> = con.keys(format!("*:{event_id}*")).await.unwrap();
    if !keys.is_empty() {
        let _: () = con.del(keys).await.unwrap();
    }
}

#[tokio::test]
async fn second_reserve_of_a_seat_is_refused() {
    let mut con = connect().await;
    let e = event(&mut con).await;
    admit(&mut con, e, 1).await;
    admit(&mut con, e, 2).await;

    assert!(matches!(reserve(&mut con, e, 1, &[3, 4]).await, ReserveOutcome::Held { .. }));
    // All or nothing: seat 5 is free but 4 is not, so neither is held
    assert_eq!(reserve(&mut con, e, 2, &[5, 4]).await, ReserveOutcome::SeatTaken(4));
    assert!(!con.getbit::<_, bool>(cache::seats_key(e), 5).await.unwrap());
    assert_eq!(get_i64(&mut con, cache::available_key(e)).await, 8);
    // Losing the race does not cost the turn: the loser picks again
    assert!(matches!(reserve(&mut con, e, 2, &[5, 6]).await, ReserveOutcome::Held { .. }));
    cleanup(&mut con, e).await;
}

#[tokio::test]
async fn retry_returns_the_same_reservation() {
    let mut con = connect().await;
    let e = event(&mut con).await;
    let token = admit(&mut con, e, 1).await;

    let ReserveOutcome::Held { reservation_id, expires_at } = reserve(&mut con, e, 1, &[0]).await else { panic!("not held") };
    assert_eq!(reserve(&mut con, e, 1, &[0]).await, ReserveOutcome::Existing { reservation_id, expires_at });
    // The token is used and its active slot freed for the next person
    let status: String = con.hget(cache::token_key(&token), "status").await.unwrap();
    assert_eq!(status, "USED");
    assert_eq!(con.zscore::<_, _, Option<f64>>(format!("active_tokens:{e}"), &token).await.unwrap(), None);
    cleanup(&mut con, e).await;
}

#[tokio::test]
async fn refuses_unadmitted_invalid_too_many_and_closed() {
    let mut con = connect().await;
    let e = event(&mut con).await;
    assert_eq!(reserve(&mut con, e, 1, &[0]).await, ReserveOutcome::NotAdmitted);

    admit(&mut con, e, 1).await;
    assert_eq!(reserve(&mut con, e, 1, &[10]).await, ReserveOutcome::InvalidSeat("10".into()));
    assert_eq!(reserve(&mut con, e, 1, &[0, 1, 2, 3, 4]).await, ReserveOutcome::TooMany { held: 0 });

    let _: () = con.set(cache::closed_key(e), 1).await.unwrap();
    assert_eq!(reserve(&mut con, e, 1, &[0]).await, ReserveOutcome::Closed);
    cleanup(&mut con, e).await;
}

#[tokio::test]
async fn release_frees_seats_but_the_turn_stays_used() {
    let mut con = connect().await;
    let e = event(&mut con).await;
    let token = admit(&mut con, e, 1).await;
    let ReserveOutcome::Held { reservation_id, .. } = reserve(&mut con, e, 1, &[2, 7]).await else { panic!("not held") };

    assert_eq!(cache::release_hold(&mut con, &reservation_id, Some(2)).await.unwrap(), ReleaseOutcome::Forbidden);
    assert_eq!(cache::release_hold(&mut con, &reservation_id, Some(1)).await.unwrap(), ReleaseOutcome::Released);
    assert_eq!(cache::release_hold(&mut con, &reservation_id, Some(1)).await.unwrap(), ReleaseOutcome::NotFound);

    assert!(!con.getbit::<_, bool>(cache::seats_key(e), 2).await.unwrap());
    assert!(!con.getbit::<_, bool>(cache::seats_key(e), 7).await.unwrap());
    assert_eq!(get_i64(&mut con, cache::available_key(e)).await, 10);
    assert_eq!(get_i64(&mut con, format!("user_seats:{e}:1")).await, 0);
    assert_eq!(con.zscore::<_, _, Option<f64>>(cache::HOLDS_EXPIRY_KEY, &reservation_id).await.unwrap(), None);
    // The turn was spent on the reservation
    let status: String = con.hget(cache::token_key(&token), "status").await.unwrap();
    assert_eq!(status, "USED");
    assert_eq!(con.zscore::<_, _, Option<f64>>(format!("active_tokens:{e}"), &token).await.unwrap(), None);
    assert_eq!(reserve(&mut con, e, 1, &[2]).await, ReserveOutcome::TurnUsed);
    cleanup(&mut con, e).await;
}

async fn join(con: &mut RedisConn, event_id: i32, user_id: i32) -> JoinOutcome {
    cache::join_waiting_room(con, event_id, user_id, &uuid::Uuid::new_v4().to_string()).await.unwrap()
}

#[tokio::test]
async fn rejoin_only_after_the_hold_ends() {
    let mut con = connect().await;
    let e = event(&mut con).await;
    let _: () = con.set(format!("lobby_flushed:{e}"), 1).await.unwrap();
    let token = admit(&mut con, e, 1).await;
    let ReserveOutcome::Held { reservation_id, .. } = reserve(&mut con, e, 1, &[0]).await else { panic!("not held") };

    // Holding seats: joining returns the used token, not a new place in line
    let JoinOutcome::Existing(existing) = join(&mut con, e, 1).await else { panic!("expected the existing token") };
    assert_eq!((existing.token_id.as_str(), existing.status.as_str()), (token.as_str(), "USED"));

    cache::release_hold(&mut con, &reservation_id, Some(1)).await.unwrap();
    let JoinOutcome::Created(new) = join(&mut con, e, 1).await else { panic!("expected a new token") };
    assert_ne!(new.token_id, token);
    assert_eq!(new.status, "QUEUED");
    // Idempotent again for the new token
    let JoinOutcome::Existing(again) = join(&mut con, e, 1).await else { panic!("expected the new token") };
    assert_eq!(again.token_id, new.token_id);
    cleanup(&mut con, e).await;
}

#[tokio::test]
async fn rejoining_keeps_the_seat_limit() {
    let mut con = connect().await;
    let e = event(&mut con).await;
    let _: () = con.set(format!("lobby_flushed:{e}"), 1).await.unwrap();
    admit(&mut con, e, 1).await;
    let ReserveOutcome::Held { reservation_id, .. } = reserve(&mut con, e, 1, &[0, 1]).await else { panic!("not held") };
    // Paid, as the payment worker will do: the hold ends, the seats stay counted
    let _: () = con.del(format!("resv:{reservation_id}")).await.unwrap();

    assert!(matches!(join(&mut con, e, 1).await, JoinOutcome::Created(_)));
    admit(&mut con, e, 1).await;
    assert_eq!(reserve(&mut con, e, 1, &[2, 3, 4]).await, ReserveOutcome::TooMany { held: 2 });
    assert!(matches!(reserve(&mut con, e, 1, &[2, 3]).await, ReserveOutcome::Held { .. }));
    cleanup(&mut con, e).await;
}

/// A payment reported while its hold is live wins even if the payment worker
/// only gets to it after the hold would have expired: the hold is claimed on
/// receipt, so expiry cannot take it and the worker's own claim still wins.
/// A payment claimed only after the hold ended is refunded (the control).
#[tokio::test]
async fn payment_claimed_on_receipt_survives_a_worker_backlog() {
    let mut con = connect().await;
    let e = event(&mut con).await;
    admit(&mut con, e, 1).await;
    admit(&mut con, e, 2).await;
    let mut sync = redis::Client::open(std::env::var("TEST_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379/15".into()))
        .unwrap()
        .get_connection()
        .unwrap();

    // Two holds of 300 ms, both paying (extended by 200 ms)
    let (paid, late) = (uuid::Uuid::new_v4().to_string(), uuid::Uuid::new_v4().to_string());
    for (resv, user, seat) in [(&paid, 1, 1), (&late, 2, 2)] {
        let held = cache::reserve_seats(&mut con, e, user, resv, &[seat], 300, MAX_SEATS).await.unwrap();
        assert!(matches!(held, ReserveOutcome::Held { .. }));
        assert!(matches!(cache::start_payment(&mut con, resv, user, 200).await.unwrap(), cache::StartPaymentOutcome::Paying { .. }));
    }

    // The first payment's success event arrives in time and is claimed at once
    assert!(matches!(cache::claim_hold(&mut sync, &paid, "pi_paid").unwrap(), cache::ClaimOutcome::Won(_)));

    // The worker is behind: both holds pass their expiry, and cleanup runs
    tokio::time::sleep(std::time::Duration::from_millis(800)).await;
    cache::claim_expired(&mut sync, 500).unwrap();

    // The claimed hold kept its seat and is still there for the worker...
    assert!(con.getbit::<_, bool>(cache::seats_key(e), 1).await.unwrap());
    assert!(con.exists::<_, bool>(format!("resv:{paid}")).await.unwrap());
    // ...whose claim, with the same intent, wins
    assert!(matches!(cache::claim_hold(&mut sync, &paid, "pi_paid").unwrap(), cache::ClaimOutcome::Won(_)));
    // It is kept far longer than the hold's own expiry plus grace
    let ttl: i64 = con.pttl(format!("resv:{paid}")).await.unwrap();
    assert!(ttl > 60 * 60 * 1000, "claimed hold TTL {ttl} ms");

    // The unclaimed one expired and was released: its payment is refunded
    assert!(!con.getbit::<_, bool>(cache::seats_key(e), 2).await.unwrap());
    assert_eq!(cache::claim_hold(&mut sync, &late, "pi_late").unwrap(), cache::ClaimOutcome::Gone);
    // Another payment for the claimed hold does not win it
    assert_eq!(cache::claim_hold(&mut sync, &paid, "pi_other").unwrap(), cache::ClaimOutcome::Gone);

    let _: () = con.del(&[format!("resv:{paid}"), format!("resv:{late}")]).await.unwrap();
    cleanup(&mut con, e).await;
}
