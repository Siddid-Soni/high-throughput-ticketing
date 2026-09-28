use std::collections::{BTreeSet, HashMap};

use actix_web::{get, http::header, post, web, HttpRequest, HttpResponse};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
    actions,
    auth::AuthUser,
    cache::{self, AvailabilityOutcome, RedisConn},
    db::DbPool,
    error::ApiError,
    models::{Event, EventSeatDetail, NewEvent},
    waiting::SharedQueue,
};

#[derive(Debug, Deserialize)]
struct CreateEventRequest {
    venue_id: i32,
    name: String,
    start_time: DateTime<Utc>,
    /// When the waiting room starts admitting; joins before this wait in a lobby
    sale_opens_at: DateTime<Utc>,
    /// Price in cents for every section of the venue
    prices: HashMap<String, i32>,
}

#[derive(Debug, Serialize)]
struct CreateEventResponse {
    #[serde(flatten)]
    event: Event,
    seat_count: usize,
}

#[post("/events")]
async fn create_event(
    pool: web::Data<DbPool>,
    redis: web::Data<RedisConn>,
    queue: web::Data<SharedQueue>,
    body: web::Json<CreateEventRequest>,
) -> Result<HttpResponse, ApiError> {
    let body = body.into_inner();
    if body.name.trim().is_empty() {
        return Err(ApiError::BadRequest("name must not be empty".into()));
    }
    if body.start_time <= Utc::now() {
        return Err(ApiError::BadRequest("start_time must be in the future".into()));
    }
    if body.sale_opens_at <= Utc::now() {
        return Err(ApiError::BadRequest("sale_opens_at must be in the future".into()));
    }
    if body.sale_opens_at >= body.start_time {
        return Err(ApiError::BadRequest("sale_opens_at must be before start_time".into()));
    }
    if let Some((section, _)) = body.prices.iter().find(|(_, price)| **price < 0) {
        return Err(ApiError::BadRequest(format!("price for section '{section}' must not be negative")));
    }

    let (event, seat_count) = web::block(move || {
        let mut conn = pool.get()?;

        let sections = actions::venues::venue_sections(&mut conn, body.venue_id).map_err(|err| match err {
            diesel::result::Error::NotFound => ApiError::NotFound(format!("venue {} not found", body.venue_id)),
            other => other.into(),
        })?;
        check_prices_cover_sections(&sections, &body.prices)?;

        let new_event = NewEvent {
            venue_id: body.venue_id,
            name: &body.name,
            start_time: body.start_time,
            sale_opens_at: body.sale_opens_at,
        };
        let created = actions::events::create_event(&mut conn, &new_event, &body.prices).map_err(|err| {
            match ApiError::from(err) {
                ApiError::Conflict(_) => ApiError::Conflict(format!("an event named '{}' already exists", body.name)),
                other => other,
            }
        })?;
        Ok::<_, ApiError>(created)
    })
    .await??;

    // Only after the commit: a rolled-back event must not get a seat map or topic.
    // If this fails, the event exists without them until the startup rebuild
    // (step 7.4) or the first join recreates them.
    let mut con = redis.get_ref().clone();
    cache::init_event(&mut con, event.id, seat_count, event.sale_opens_at).await?;
    // Idempotent; the join endpoint calls it too, so a topic missed here is created on first join
    queue.ensure_event(event.id).await?;

    Ok(HttpResponse::Created().json(CreateEventResponse { event, seat_count }))
}

#[derive(Debug, Serialize)]
struct LayoutResponse {
    event_id: i32,
    seat_count: usize,
    /// Ordered by seat_index, so seats[i] is bit i of the availability bitmap
    seats: Vec<EventSeatDetail>,
}

/// Seat details for an event. They never change once the event exists, so
/// CDNs and browsers may cache the response indefinitely.
#[get("/events/{id}/layout")]
async fn get_layout(pool: web::Data<DbPool>, path: web::Path<i32>) -> Result<HttpResponse, ApiError> {
    let event_id = path.into_inner();

    let seats = web::block(move || {
        let mut conn = pool.get()?;
        actions::events::event_seat_details(&mut conn, event_id).map_err(|err| match err {
            diesel::result::Error::NotFound => ApiError::NotFound(format!("event {event_id} not found")),
            other => other.into(),
        })
    })
    .await??;

    Ok(HttpResponse::Ok()
        .insert_header((header::CACHE_CONTROL, "public, max-age=31536000, immutable"))
        .json(LayoutResponse { event_id, seat_count: seats.len(), seats }))
}

/// The raw Redis bitmap: bit i (most significant bit of each byte first) is 1
/// when seat_index i is taken. Only for admitted users (an ACTIVE waiting room
/// token for the event), so the queued crowd never polls it. Not cached by CDNs
/// or proxies; clients revalidate every time and get `304` with no body when
/// the ETag still matches. Never touches Postgres.
#[get("/events/{id}/availability")]
async fn get_availability(
    user: AuthUser,
    redis: web::Data<RedisConn>,
    path: web::Path<i32>,
    req: HttpRequest,
) -> Result<HttpResponse, ApiError> {
    let event_id = path.into_inner();

    let mut con = redis.get_ref().clone();
    let (bits, available) = match cache::load_availability(&mut con, event_id, user.id).await? {
        AvailabilityOutcome::Ok { bits, available } => (bits, available),
        AvailabilityOutcome::NotAdmitted => {
            return Err(ApiError::Forbidden(format!(
                "seat availability is only shown to users admitted from the waiting room for event {event_id}"
            )))
        }
        AvailabilityOutcome::NoSeatMap => return Err(ApiError::NotFound(format!("no seat map for event {event_id}"))),
    };

    let etag = format!("\"{:016x}\"", fnv1a(&bits));
    let cache_control = (header::CACHE_CONTROL, "private, no-cache");

    if if_none_match(&req, &etag) {
        return Ok(HttpResponse::NotModified()
            .insert_header((header::ETAG, etag))
            .insert_header(cache_control)
            .finish());
    }

    Ok(HttpResponse::Ok()
        .content_type("application/octet-stream")
        .insert_header((header::ETAG, etag))
        .insert_header(cache_control)
        .insert_header(("X-Available-Count", available.to_string()))
        .body(bits))
}

fn if_none_match(req: &HttpRequest, etag: &str) -> bool {
    req.headers()
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| {
            value.split(',').map(|tag| tag.trim().trim_start_matches("W/")).any(|tag| tag == etag || tag == "*")
        })
}

// FNV-1a: stable across processes and builds, so every API instance produces
// the same ETag for the same bitmap
fn fnv1a(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf29ce484222325, |hash, byte| (hash ^ *byte as u64).wrapping_mul(0x100000001b3))
}

/// Every venue section needs a price, and every priced section must exist.
fn check_prices_cover_sections(sections: &[String], prices: &HashMap<String, i32>) -> Result<(), ApiError> {
    let venue: BTreeSet<&str> = sections.iter().map(String::as_str).collect();
    let priced: BTreeSet<&str> = prices.keys().map(String::as_str).collect();

    let missing: Vec<&str> = venue.difference(&priced).copied().collect();
    if !missing.is_empty() {
        return Err(ApiError::BadRequest(format!("missing price for sections: {}", missing.join(", "))));
    }
    let unknown: Vec<&str> = priced.difference(&venue).copied().collect();
    if !unknown.is_empty() {
        return Err(ApiError::BadRequest(format!("venue has no sections named: {}", unknown.join(", "))));
    }
    Ok(())
}

// POST /events, GET /events/{id}/layout, GET /events/{id}/availability
#[derive(Debug, Serialize)]
struct CloseResponse {
    event_id: i32,
    status: &'static str,
}

/// Ends the sale early (the admission worker otherwise closes it at the start
/// time). Final and idempotent: joins then return `410`, waiting tokens report
/// CLOSED, and the worker stops. The event check is in Redis, like joins.
#[post("/events/{id}/close")]
async fn close_event(
    pool: web::Data<DbPool>,
    redis: web::Data<RedisConn>,
    path: web::Path<i32>,
) -> Result<HttpResponse, ApiError> {
    let event_id = path.into_inner();

    // Redis first: the flag turns joins away and stops admission at once. If
    // Postgres then fails, the sale is still closed; a retry, or the admission
    // worker when it sees the flag, records it. Postgres is updated even when
    // the flag was already set, so a retry does exactly that.
    let mut con = redis.get_ref().clone();
    if !cache::close_existing_event(&mut con, event_id).await? {
        return Err(ApiError::NotFound(format!("event {event_id} not found")));
    }
    // Leaves a canceled event canceled
    web::block(move || {
        let mut conn = pool.get()?;
        Ok::<_, ApiError>(actions::events::close_sale(&mut conn, event_id)?)
    })
    .await??;

    Ok(HttpResponse::Ok().json(CloseResponse { event_id, status: "CLOSED" }))
}

pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(create_event).service(get_layout).service(get_availability).service(close_event);
}
