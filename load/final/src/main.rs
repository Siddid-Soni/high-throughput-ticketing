//! Simulated buyers for the final load test (step 8.3). Each user joins the
//! waiting room, polls its status, and once admitted picks a random free seat
//! from the availability bitmap and reserves it (retrying when someone was
//! faster). Then it pays (`PAY_RATIO` of users) or abandons the hold. Users
//! give up once the status reports `sold_out`.
//!
//! `MODE=poll` measures capacity instead: USERS users (ids from USER_ID_START,
//! no database rows needed) join within JOIN_SPREAD_MS, then poll their
//! status every POLL_MS until DURATION_SECS have passed.
//!
//! Environment: BASE_URL (several, comma-separated, are used round-robin by
//! user), EVENT_ID, SEAT_COUNT, USER_IDS (a file of user ids, one per line),
//! JWT_SECRET, and optionally PAY_RATIO (0.7), POLL_MS (2000), DEADLINE_SECS
//! (900). Prints a report; exit code 1 if any request failed with a 5xx or a
//! network error.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::Value;

struct Ctx {
    client: reqwest::Client,
    bases: Vec<String>,
    event_id: i32,
    seat_count: usize,
    pay_ratio: f64,
    poll: Duration,
    deadline: Instant,
    secret: String,
    samples: Mutex<Vec<(&'static str, u16, Duration)>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum Outcome {
    Paid,
    Abandoned,
    SoldOut,
    Closed,
    /// Admitted but every seat tried was taken, or none was free
    NoSeat,
    /// The token ran out before reserving
    Expired,
    Timeout,
    Error(String),
}

impl Ctx {
    /// Sends a request and records its status (0 for a network error) and latency
    async fn call(&self, endpoint: &'static str, request: reqwest::RequestBuilder) -> (u16, Vec<u8>) {
        let started = Instant::now();
        let result = async {
            let response = request.send().await?;
            let status = response.status().as_u16();
            Ok::<_, reqwest::Error>((status, response.bytes().await?.to_vec()))
        }
        .await;
        let (status, body) = result.unwrap_or((0, Vec::new()));
        self.samples.lock().unwrap().push((endpoint, status, started.elapsed()));
        (status, body)
    }

    fn url(&self, path: &str) -> String {
        self.url_for(0, path)
    }

    /// Spreads users over the API addresses
    fn url_for(&self, user_id: i32, path: &str) -> String {
        format!("{}{}", self.bases[user_id as usize % self.bases.len()], path)
    }
}

async fn sleep_ms(min: u64, max: u64) {
    tokio::time::sleep(Duration::from_millis(fastrand::u64(min..=max))).await;
}

async fn user(ctx: Arc<Ctx>, user_id: i32) -> Outcome {
    let (jwt, _) = conference::auth::issue_token(user_id, &ctx.secret, 3600).expect("sign a token");
    let bearer = format!("Bearer {jwt}");
    let e = ctx.event_id;

    // Everyone arrives in the first few seconds, before the sale opens
    sleep_ms(0, 5000).await;
    let (code, _) =
        ctx.call("join", ctx.client.post(ctx.url(&format!("/waiting-room/{e}/join"))).header("Authorization", &bearer)).await;
    if code != 202 {
        return Outcome::Error(format!("join {code}"));
    }

    let poll_ms = ctx.poll.as_millis() as u64;
    loop {
        if Instant::now() > ctx.deadline {
            return Outcome::Timeout;
        }
        sleep_ms(poll_ms, poll_ms * 3 / 2).await;
        let (code, body) = ctx
            .call("status", ctx.client.get(ctx.url(&format!("/waiting-room/{e}/status"))).header("Authorization", &bearer))
            .await;
        if code != 200 {
            continue;
        }
        let status: Value = serde_json::from_slice(&body).unwrap_or_default();
        match status["status"].as_str().unwrap_or("") {
            "LOBBY" | "QUEUED" if status["reason"] == "sold_out" => return Outcome::SoldOut,
            "LOBBY" | "QUEUED" => {}
            "ACTIVE" => return shop(&ctx, &bearer).await,
            "CLOSED" => return Outcome::Closed,
            "EXPIRED" => return Outcome::Expired,
            other => return Outcome::Error(format!("status {other}")),
        }
    }
}

/// Capacity mode: join, then poll status at a steady rate until the deadline
async fn poller(ctx: Arc<Ctx>, user_id: i32, join_spread_ms: u64) -> Outcome {
    let (jwt, _) = conference::auth::issue_token(user_id, &ctx.secret, 3600).expect("sign a token");
    let bearer = format!("Bearer {jwt}");
    let e = ctx.event_id;
    sleep_ms(0, join_spread_ms).await;
    let (code, _) = ctx
        .call("join", ctx.client.post(ctx.url_for(user_id, &format!("/waiting-room/{e}/join"))).header("Authorization", &bearer))
        .await;
    if code != 202 {
        return Outcome::Error(format!("join {code}"));
    }
    let poll_ms = ctx.poll.as_millis() as u64;
    // Spread the first poll so the users do not move in lockstep
    sleep_ms(0, poll_ms).await;
    while Instant::now() < ctx.deadline {
        ctx.call("status", ctx.client.get(ctx.url_for(user_id, &format!("/waiting-room/{e}/status"))).header("Authorization", &bearer))
            .await;
        sleep_ms(poll_ms * 9 / 10, poll_ms * 11 / 10).await;
    }
    Outcome::Timeout
}

/// Picks and reserves a seat, then pays or walks away
async fn shop(ctx: &Ctx, bearer: &str) -> Outcome {
    let e = ctx.event_id;
    for _ in 0..5 {
        let (code, bits) =
            ctx.call("availability", ctx.client.get(ctx.url(&format!("/events/{e}/availability"))).header("Authorization", bearer)).await;
        match code {
            200 => {}
            403 => return Outcome::Expired,
            _ => return Outcome::Error(format!("availability {code}")),
        }
        let free: Vec<usize> =
            (0..ctx.seat_count).filter(|&i| bits.get(i / 8).is_some_and(|byte| byte & (0x80 >> (i % 8)) == 0)).collect();
        if free.is_empty() {
            return Outcome::NoSeat;
        }
        let seat = free[fastrand::usize(..free.len())];
        // A moment to choose
        sleep_ms(200, 1000).await;

        let (code, body) = ctx
            .call(
                "reserve",
                ctx.client
                    .post(ctx.url("/reserve"))
                    .header("Authorization", bearer)
                    .json(&serde_json::json!({ "event_id": e, "seat_indexes": [seat] })),
            )
            .await;
        match code {
            201 => {
                let reservation: Value = serde_json::from_slice(&body).unwrap_or_default();
                let id = reservation["reservation_id"].as_str().unwrap_or("").to_owned();
                if fastrand::f64() >= ctx.pay_ratio {
                    return Outcome::Abandoned;
                }
                sleep_ms(200, 1000).await;
                let (code, _) = ctx
                    .call(
                        "pay",
                        ctx.client
                            .post(ctx.url(&format!("/pay/{id}")))
                            .header("Authorization", bearer)
                            .header("Idempotency-Key", uuid::Uuid::new_v4().to_string()),
                    )
                    .await;
                return if code == 200 { Outcome::Paid } else { Outcome::Error(format!("pay {code}")) };
            }
            // Someone was faster: look again
            409 => continue,
            403 => return Outcome::Expired,
            _ => return Outcome::Error(format!("reserve {code}")),
        }
    }
    Outcome::NoSeat
}

fn percentile(sorted: &[Duration], p: f64) -> f64 {
    if sorted.is_empty() {
        return 0.0;
    }
    let index = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[index].as_secs_f64() * 1000.0
}

#[tokio::main]
async fn main() {
    let var = |key: &str| std::env::var(key).unwrap_or_else(|_| panic!("{key} must be set"));
    let opt = |key: &str, default: &str| std::env::var(key).unwrap_or_else(|_| default.to_owned());
    let poll_mode = opt("MODE", "buy") == "poll";
    let users: Vec<i32> = if poll_mode {
        let start: i32 = opt("USER_ID_START", "10000000").parse().unwrap();
        (start..start + var("USERS").parse::<i32>().unwrap()).collect()
    } else {
        std::fs::read_to_string(var("USER_IDS")).expect("read USER_IDS").lines().filter_map(|l| l.trim().parse().ok()).collect()
    };
    let deadline_secs = if poll_mode { var("DURATION_SECS") } else { opt("DEADLINE_SECS", "900") };
    let join_spread_ms: u64 = opt("JOIN_SPREAD_MS", "5000").parse().unwrap();

    let started = Instant::now();
    let ctx = Arc::new(Ctx {
        client: reqwest::Client::builder().pool_max_idle_per_host(4096).timeout(Duration::from_secs(30)).build().unwrap(),
        bases: var("BASE_URL").split(',').map(str::to_owned).collect(),
        event_id: var("EVENT_ID").parse().unwrap(),
        seat_count: var("SEAT_COUNT").parse().unwrap(),
        pay_ratio: opt("PAY_RATIO", "0.7").parse().unwrap(),
        poll: Duration::from_millis(opt("POLL_MS", "2000").parse().unwrap()),
        deadline: started + Duration::from_secs(deadline_secs.parse().unwrap()),
        secret: var("JWT_SECRET"),
        samples: Mutex::new(Vec::new()),
    });
    println!("{} users, event {}, {} seats", users.len(), ctx.event_id, ctx.seat_count);

    let tasks: Vec<_> = users
        .iter()
        .map(|&id| {
            if poll_mode {
                tokio::spawn(poller(ctx.clone(), id, join_spread_ms))
            } else {
                tokio::spawn(user(ctx.clone(), id))
            }
        })
        .collect();
    let mut outcomes: BTreeMap<Outcome, usize> = BTreeMap::new();
    for task in tasks {
        *outcomes.entry(task.await.unwrap_or(Outcome::Error("task panicked".into()))).or_default() += 1;
    }
    let elapsed = started.elapsed();

    println!("\nFinished in {:.1}s\n\nOutcomes:", elapsed.as_secs_f64());
    for (outcome, count) in &outcomes {
        println!("  {outcome:?}: {count}");
    }

    let samples = std::mem::take(&mut *ctx.samples.lock().unwrap());
    let mut by_endpoint: HashMap<&str, (Vec<Duration>, BTreeMap<u16, usize>)> = HashMap::new();
    for (endpoint, status, latency) in &samples {
        let entry = by_endpoint.entry(endpoint).or_default();
        entry.0.push(*latency);
        *entry.1.entry(*status).or_default() += 1;
    }
    println!("\n| endpoint | requests | req/s | p50 ms | p95 ms | p99 ms | max ms | responses |");
    println!("|---|---|---|---|---|---|---|---|");
    let mut failed = 0;
    for endpoint in ["join", "status", "availability", "reserve", "pay"] {
        let Some((latencies, codes)) = by_endpoint.get_mut(endpoint) else { continue };
        latencies.sort_unstable();
        failed += codes.iter().filter(|(code, _)| **code == 0 || **code >= 500).map(|(_, n)| n).sum::<usize>();
        let codes = codes.iter().map(|(code, n)| format!("{code}: {n}")).collect::<Vec<_>>().join(", ");
        println!(
            "| {endpoint} | {} | {:.0} | {:.1} | {:.1} | {:.1} | {:.1} | {codes} |",
            latencies.len(),
            latencies.len() as f64 / elapsed.as_secs_f64(),
            percentile(latencies, 0.50),
            percentile(latencies, 0.95),
            percentile(latencies, 0.99),
            percentile(latencies, 1.0),
        );
    }
    println!("\nServer errors and network failures: {failed}");
    if failed > 0 {
        std::process::exit(1);
    }
}
