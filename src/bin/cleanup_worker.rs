//! Cleanup worker: releases seat holds whose time is up, and restores an
//! event's seat map if Redis lost it. Any number can run at once: the claim
//! script hands each expired hold to exactly one worker.
//!
//! Usage: `cargo run --bin cleanup_worker`

use std::collections::HashMap;
use std::time::{Duration, Instant};

use chrono::Utc;
use uuid::Uuid;

use conference::{
    actions,
    cache::{self, RebuildInput},
    config::Config,
    db::{self, DbPool},
};

// Holds released per claim; a full batch is followed at once by another
const BATCH: usize = 500;
const IDLE_WAIT: Duration = Duration::from_millis(500);
const ERROR_RETRY: Duration = Duration::from_secs(1);
// How often to look for events whose seat map is missing
const REBUILD_EVERY: Duration = Duration::from_secs(10);
// How often to expire bookings a crashed worker left PAYMENT_PENDING, and how
// long after its hold ended a booking must be to count as left behind
const SWEEP_EVERY: Duration = Duration::from_secs(60);
const SWEEP_AFTER: chrono::Duration = chrono::Duration::minutes(10);

type BoxError = Box<dyn std::error::Error + Send + Sync>;

fn main() {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));
    let config = Config::from_env();
    let pool = db::init_pool(&config);
    let client = redis::Client::open(config.redis_url.as_str()).expect("REDIS_URL should be valid");
    let mut redis: Option<redis::Connection> = None;
    // Bookings to mark EXPIRED, kept until Postgres accepts them
    let mut unmarked: Vec<Uuid> = Vec::new();
    let (mut last_rebuild, mut last_sweep) = (None::<Instant>, None::<Instant>);
    log::info!("Releasing expired holds");

    loop {
        let con = match redis.as_mut() {
            Some(con) => con,
            None => match client.get_connection() {
                Ok(con) => redis.insert(con),
                Err(e) => {
                    log::error!("Connecting to Redis failed: {e}");
                    std::thread::sleep(ERROR_RETRY);
                    continue;
                }
            },
        };

        if last_rebuild.is_none_or(|at| at.elapsed() >= REBUILD_EVERY) {
            last_rebuild = Some(Instant::now());
            if let Err(e) = rebuild_missing(&pool, con) {
                log::error!("Checking for lost seat maps failed: {e}");
            }
        }
        if last_sweep.is_none_or(|at| at.elapsed() >= SWEEP_EVERY) {
            last_sweep = Some(Instant::now());
            match pool.get().map_err(BoxError::from).and_then(|mut conn| {
                Ok(actions::bookings::expire_stale(&mut conn, Utc::now() - SWEEP_AFTER)?)
            }) {
                Ok(0) => {}
                Ok(n) => log::warn!("Expired {n} bookings left PAYMENT_PENDING after their hold ended"),
                Err(e) => log::error!("Sweeping stale bookings failed: {e}"),
            }
        }

        let full = match cache::claim_expired(con, BATCH) {
            Ok((scanned, released)) => {
                if !released.is_empty() {
                    log::info!("Released {} expired holds", released.len());
                }
                unmarked.extend(
                    released.iter().filter(|hold| hold.state == "paying").filter_map(|hold| hold.reservation_id.parse::<Uuid>().ok()),
                );
                scanned == BATCH
            }
            Err(e) => {
                log::error!("Claiming expired holds failed: {e}");
                redis = None;
                std::thread::sleep(ERROR_RETRY);
                continue;
            }
        };

        // The seats are free in Redis already; the bookings follow
        if !unmarked.is_empty() {
            match pool.get().map_err(BoxError::from).and_then(|mut conn| {
                Ok(actions::bookings::expire_payment_pending(&mut conn, &unmarked)?)
            }) {
                Ok(n) => {
                    if n > 0 {
                        log::info!("Marked {n} bookings EXPIRED");
                    }
                    unmarked.clear();
                }
                Err(e) => {
                    log::error!("Marking {} bookings EXPIRED failed, will retry: {e}", unmarked.len());
                    std::thread::sleep(ERROR_RETRY);
                    continue;
                }
            }
        }

        if !full {
            std::thread::sleep(IDLE_WAIT);
        }
    }
}

/// Rebuilds every live event whose seat map is missing from Redis (lost
/// data, or Redis restarted without its AOF). Cheap when nothing is missing:
/// one query and one pipelined `EXISTS` per event.
fn rebuild_missing(pool: &DbPool, redis: &mut redis::Connection) -> Result<(), BoxError> {
    let mut conn = pool.get()?;
    let events = actions::events::live_events(&mut conn)?;
    if events.is_empty() {
        return Ok(());
    }
    let mut pipe = redis::pipe();
    for event in &events {
        pipe.exists(cache::seat_count_key(event.id));
    }
    let present: Vec<bool> = pipe.query(redis)?;

    for (event, _) in events.iter().zip(present).filter(|(_, present)| !present) {
        // One worker rebuilds; the script would refuse a second run anyway
        let locked: Option<String> =
            redis::cmd("SET").arg(format!("rebuild_lock:{}", event.id)).arg(1).arg("NX").arg("PX").arg(30_000).query(redis)?;
        if locked.is_none() {
            continue;
        }
        let (seat_count, taken, sold) = actions::events::seat_state(&mut conn, event.id)?;
        let user_seats: HashMap<String, i64> = actions::bookings::paid_seats_by_user(&mut conn, event.id)?
            .into_iter()
            .map(|(user, count)| (user.to_string(), count))
            .collect();
        let input = RebuildInput {
            seat_count,
            sold,
            sale_opens_at: event.sale_opens_at,
            closed: event.status == "CLOSED",
            on_sale: event.status != "SCHEDULED",
            taken,
            holds: cache::live_holds(redis, event.id)?,
            user_seats,
        };
        if cache::rebuild_event(redis, event.id, &input)? {
            log::warn!(
                "Rebuilt the seat map of event {} from Postgres: {} seats, {} sold or blocked, {} live holds",
                event.id,
                seat_count,
                input.taken.len(),
                input.holds.len()
            );
        }
    }
    Ok(())
}
