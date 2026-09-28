use actix_web::{delete, post, web, HttpResponse};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    auth::AuthUser,
    cache::{self, RedisConn, ReleaseOutcome, ReserveOutcome},
    config::Config,
    error::ApiError,
};

#[derive(Debug, Deserialize)]
struct ReserveRequest {
    event_id: i32,
    seat_indexes: Vec<u32>,
}

#[derive(Debug, Serialize)]
struct ReserveResponse {
    reservation_id: String,
    /// When the hold ends unless payment starts first
    expires_at: DateTime<Utc>,
}

/// Holds seats for the caller, whose waiting room token for the event must be
/// ACTIVE (found through `user_token`, like the status endpoint). A refusal
/// (seat taken, limit, invalid seat) leaves the token ACTIVE to try again; a
/// successful hold uses it up for good. Redis only,
/// all seats or none: this is the busiest moment of a sale, so Postgres is not
/// touched until payment starts (the booking and its price are recorded then;
/// clients price the seats from `/layout`). Retrying after a timeout returns
/// the same hold with `200`.
#[post("/reserve")]
async fn reserve(
    user: AuthUser,
    redis: web::Data<RedisConn>,
    config: web::Data<Config>,
    body: web::Json<ReserveRequest>,
) -> Result<HttpResponse, ApiError> {
    let ReserveRequest { event_id, seat_indexes } = body.into_inner();
    if seat_indexes.is_empty() {
        return Err(ApiError::BadRequest("seat_indexes must not be empty".into()));
    }
    let mut unique = seat_indexes.clone();
    unique.sort_unstable();
    unique.dedup();
    if unique.len() != seat_indexes.len() {
        return Err(ApiError::BadRequest("seat_indexes must not repeat a seat".into()));
    }
    if seat_indexes.len() > config.max_seats_per_user as usize {
        return Err(ApiError::BadRequest(format!("at most {} seats per user", config.max_seats_per_user)));
    }

    let mut con = redis.get_ref().clone();
    let outcome = cache::reserve_seats(
        &mut con,
        event_id,
        user.id,
        &Uuid::new_v4().to_string(),
        &seat_indexes,
        config.hold_ttl_secs * 1000,
        config.max_seats_per_user,
    )
    .await?;

    match outcome {
        ReserveOutcome::Held { reservation_id, expires_at } => {
            Ok(HttpResponse::Created().json(ReserveResponse { reservation_id, expires_at }))
        }
        ReserveOutcome::Existing { reservation_id, expires_at } => {
            Ok(HttpResponse::Ok().json(ReserveResponse { reservation_id, expires_at }))
        }
        ReserveOutcome::Closed => Err(ApiError::Gone(format!("the sale for event {event_id} is closed"))),
        ReserveOutcome::NotAdmitted => {
            Err(ApiError::Forbidden("wait for your turn in the waiting room before reserving".into()))
        }
        ReserveOutcome::TurnUsed => {
            Err(ApiError::Forbidden("your turn was used for an earlier reservation, which has ended".into()))
        }
        ReserveOutcome::InvalidSeat(index) => Err(ApiError::BadRequest(format!("seat {index} is not in event {event_id}"))),
        ReserveOutcome::TooMany { held } => Err(ApiError::Conflict(format!(
            "you already hold {held} seats; at most {} per user",
            config.max_seats_per_user
        ))),
        ReserveOutcome::SeatTaken(index) => Err(ApiError::Conflict(format!("seat {index} is taken"))),
    }
}

/// Releases the caller's hold (Redis only): the seats are free again. The
/// caller's turn stays spent: they cannot reserve again with it.
#[delete("/reserve/{id}")]
async fn release(user: AuthUser, redis: web::Data<RedisConn>, path: web::Path<String>) -> Result<HttpResponse, ApiError> {
    let id = Uuid::parse_str(&path.into_inner()).map_err(|_| ApiError::BadRequest("invalid reservation id".into()))?;

    let mut con = redis.get_ref().clone();
    match cache::release_hold(&mut con, &id.to_string(), Some(user.id)).await? {
        ReleaseOutcome::Released => Ok(HttpResponse::NoContent().finish()),
        ReleaseOutcome::NotFound => Err(ApiError::NotFound(format!("no seats are held under reservation {id}"))),
        ReleaseOutcome::Forbidden => Err(ApiError::Forbidden("that reservation is not yours".into())),
        ReleaseOutcome::PaymentInProgress => {
            Err(ApiError::Conflict("payment for this reservation has started; it can no longer be released".into()))
        }
    }
}

// POST /reserve, DELETE /reserve/{id}
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(reserve).service(release);
}
