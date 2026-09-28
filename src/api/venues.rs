use actix_web::{post, web, HttpResponse};
use serde::{Deserialize, Serialize};

use crate::{
    actions,
    db::DbPool,
    error::ApiError,
    models::{NewVenue, SeatMapConfig, Venue},
};

#[derive(Debug, Deserialize)]
struct CreateVenueRequest {
    name: String,
    location: String,
    seat_map_config: SeatMapConfig,
}

#[derive(Debug, Serialize)]
struct CreateVenueResponse {
    #[serde(flatten)]
    venue: Venue,
    seat_count: usize,
}

#[post("/venue")]
async fn create_venue(
    pool: web::Data<DbPool>,
    body: web::Json<CreateVenueRequest>,
) -> Result<HttpResponse, ApiError> {
    let body = body.into_inner();
    if body.name.trim().is_empty() || body.location.trim().is_empty() {
        return Err(ApiError::BadRequest("name and location must not be empty".into()));
    }
    body.seat_map_config.validate().map_err(ApiError::BadRequest)?;

    let new_venue = NewVenue {
        name: body.name,
        location: body.location,
        seat_map_config: serde_json::to_value(&body.seat_map_config)
            .map_err(|e| ApiError::Internal(format!("serialize seat map: {e}")))?,
    };

    let (venue, seat_count) = web::block(move || {
        let mut conn = pool.get()?;
        let created = actions::venues::create_venue(&mut conn, &new_venue, &body.seat_map_config)?;
        Ok::<_, ApiError>(created)
    })
    .await??;

    Ok(HttpResponse::Created().json(CreateVenueResponse { venue, seat_count }))
}

// POST /venue
pub fn configure(cfg: &mut web::ServiceConfig) {
    cfg.service(create_venue);
}
