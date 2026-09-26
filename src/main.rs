#[macro_use]
extern crate diesel;

use actix_web::{error, middleware, post, get, web, App, HttpResponse, HttpServer, Responder};
use diesel::{prelude::*, r2d2};
use regex::Regex;
use chrono::{NaiveDateTime, Utc, DateTime};
use dotenvy;
mod actions;
mod models;
mod schema;
mod queue;

type DbPool = r2d2::Pool<r2d2::ConnectionManager<PgConnection>>;

#[derive(Debug, serde::Serialize)]
struct Res {
    message: String,
}

#[derive(Debug, serde::Deserialize)]
struct ConferenceInfo {
    name: String,
}

#[derive(Debug, serde::Deserialize)]
struct ScheduleConferenceStartRequest {
    name: String,
}

#[post("/conference")]
async fn add_conference(
    pool: web::Data<DbPool>, 
    queue_service: web::Data<queue::WaitlistQueueService>,
    form: web::Json<models::NewConference>
) -> actix_web::Result<impl Responder> {
    let re = Regex::new(r"^[a-zA-Z0-9 ]*$").unwrap();    

    if re.captures(&form.name).is_none() {
        return Ok(HttpResponse::BadRequest().json(Res { message: "name should be Alphanumeric String. Spaces are the only special character allowed".to_string() }));
    } else if re.captures(&form.location).is_none() {
        return Ok(HttpResponse::BadRequest().json(Res { message: "location should be Alphanumeric String. Spaces are the only special character allowed".to_string() }));
    }
    
    if form.topics.is_empty() {
        return Ok(HttpResponse::BadRequest().json(Res { message: "At least one topic is required".to_string() }));
    }
    
    if form.topics.len() > 10 {
        return Ok(HttpResponse::BadRequest().json(Res { message: "Maximum 10 topics allowed".to_string() }));
    }
    
    for topic in &form.topics {
        if re.captures(topic).is_none() {
            return Ok(HttpResponse::BadRequest().json(Res { message: "Topics should be Alphanumeric with spaces allowed".to_string() }));
        }
    }
    
    let start_time = match NaiveDateTime::parse_from_str(&form.start, "%Y-%m-%d %H:%M:%S") {
        Ok(dt) => dt,
        Err(_) => return Ok(HttpResponse::BadRequest().json(Res { message: "start timestamp not in correct format".to_string() }))
    };

    match NaiveDateTime::parse_from_str(&form.end, "%Y-%m-%d %H:%M:%S") {
        Err(_) => return Ok(HttpResponse::BadRequest().json(Res { message: "end timestamp not in correct format".to_string() })),
        _ => ()
    }

    let conference = web::block(move || {
        let mut conn = pool.get()?;
        actions::create_new_conference(&mut conn, &form)
    })
    .await?
    .map_err(|e| {
        let detail = e.to_string();
        log::error!("Failed to add conference: {:?}", e);
        
        if let Some(diesel_error) = e.downcast_ref::<diesel::result::Error>() {
            match diesel_error {
                diesel::result::Error::DatabaseError(diesel::result::DatabaseErrorKind::UniqueViolation, _) => {
                    error::InternalError::from_response(
                        e.to_string(),
                        HttpResponse::BadRequest().json(Res { message: "conference already exists".to_owned() })
                    )
                }
                _ => error::InternalError::from_response(
                    e.to_string(),
                    HttpResponse::BadRequest().json(Res { message: detail })
                )
            }
        } else {
            error::InternalError::from_response(
                e.to_string(),
                HttpResponse::BadRequest().json(Res { message: detail })
            )
        }
    })?;

    let conference_name = conference.name.clone();
    let start_time_utc = DateTime::<Utc>::from_naive_utc_and_offset(start_time, Utc);
    let queue_service_clone = queue_service.clone();
    
    tokio::spawn(async move {
        if let Err(e) = queue_service_clone.schedule_conference_start_event(&conference_name, start_time_utc).await {
            log::error!("Failed to schedule conference start event for '{}': {:?}", conference_name, e);
        }
    });

    Ok(HttpResponse::Created().json(Res { message: "conference added successfully".to_string() }))
}

#[post("/user")]
async fn add_user(pool: web::Data<DbPool>, form: web::Json<models::NewUser>) -> actix_web::Result<impl Responder> {
    let re = Regex::new(r"^[a-zA-Z0-9]*$").unwrap();
    let topic_re = Regex::new(r"^[a-zA-Z0-9 ]*$").unwrap();
    
    if re.captures(&form.user_id).is_none() {
        return Ok(HttpResponse::BadRequest().json(Res { message: "UserID should be Alphanumeric with no special characters".to_string() }));
    }
    
    if form.topics.is_empty() {
        return Ok(HttpResponse::BadRequest().json(Res { message: "topics are required".to_string() }));
    } else if form.topics.len() > 50 {
        return Ok(HttpResponse::BadRequest().json(Res { message: "max 50 topics allowed".to_string() }));
    }
    
    for topic in &form.topics {
        if topic_re.captures(topic).is_none() {
            return Ok(HttpResponse::BadRequest().json(Res { message: "Topics should be Alphanumeric with spaces allowed".to_string() }));
        }
    }

    let _user = web::block(move || {
        let mut conn = pool.get()?;
        actions::insert_new_user(&mut conn, &form.user_id, &form.topics)
    })
    .await?
    .map_err(|e| {
        let detail = e.to_string();
        log::error!("Failed to add user: {:?}", e);
        
        if let Some(diesel_error) = e.downcast_ref::<diesel::result::Error>() {
            match diesel_error {
                diesel::result::Error::DatabaseError(diesel::result::DatabaseErrorKind::UniqueViolation, _) => {
                    error::InternalError::from_response(
                        e.to_string(),
                        HttpResponse::BadRequest().json(Res { message: "User already exists".to_owned() })
                    )
                }
                _ => error::InternalError::from_response(
                    e.to_string(),
                    HttpResponse::BadRequest().json(Res { message: detail })
                )
            }
        } else {
            error::InternalError::from_response(
                e.to_string(),
                HttpResponse::BadRequest().json(Res { message: detail })
            )
        }
    })?;

    Ok(HttpResponse::Created().json(Res { message: "User added successfully".to_string() }))
}

#[post("/book")]
async fn book_conference(
    pool: web::Data<DbPool>,
    queue_service: web::Data<queue::WaitlistQueueService>,
    form: web::Json<models::BookConferenceRequest>
) -> actix_web::Result<impl Responder> {
    let booking_result = web::block({
        let pool = pool.clone();
        let form = form.clone();
        move || {
            let mut conn = pool.get()?;
            
            let conference = actions::get_conference_by_name(&mut conn, &form.name)?;
            
            let _user = actions::get_user_by_id(&mut conn, &form.user_id)?;
            
            let now = Utc::now().naive_utc();
            if conference.start_timestamp <= now {
                return Err("Cannot book conference that has already started".into());
            }
            
            if let Some(_) = actions::check_user_has_overlapping_booking(&mut conn, &form.user_id, conference.start_timestamp, conference.end_timestamp)? {
                return Err("User has an overlapping conference booking".into());
            }
            
            let booking = actions::create_booking_atomic(&mut conn, conference.conference_id, &form.user_id)?;
            
            let booking_status = booking.status.clone();
            let booking_waitlist_position = booking.waitlist_position;
            let booking_id = booking.booking_id;
            
            if booking_status == models::BookingStatus::CONFIRMED {
                actions::remove_from_overlapping_waitlists(
                    &mut conn,
                    &form.user_id,
                    conference.start_timestamp,
                    conference.end_timestamp,
                    conference.conference_id
                )?;
            }
            
            Ok(models::BookConferenceResponse {
                booking_id,
                status: booking_status.clone(),
                message: match booking_status {
                    models::BookingStatus::CONFIRMED => "Booking confirmed successfully".to_string(),
                    models::BookingStatus::WAITLISTED => "Added to waitlist".to_string(),
                    _ => "Booking created".to_string(),
                },
                waitlist_position: booking_waitlist_position,
            })
        }
    })
    .await?
    .map_err(|e: Box<dyn std::error::Error + Send + Sync>| {
        let detail = e.to_string();
        log::error!("Failed to book conference: {:?}", e);
        error::InternalError::from_response(e, HttpResponse::BadRequest().json(Res { message: detail }))
    })?;

    if booking_result.status == models::BookingStatus::WAITLISTED {
        let queue_service_clone = queue_service.clone();
        let booking_id = booking_result.booking_id;
        let conference_name = form.name.clone();
        
        tokio::spawn(async move {
            if let Err(e) = queue_service_clone.add_to_waitlist_by_booking_id(booking_id, &conference_name).await {
                log::error!("Failed to add booking {} to waitlist queue: {:?}", booking_id, e);
                // The booking is already committed; a queue failure must not fail the request
            }
        });
    }

    Ok(HttpResponse::Created().json(booking_result))
}

#[get("/booking/{booking_id}")]
async fn get_booking_status(
    pool: web::Data<DbPool>,
    path: web::Path<i32>
) -> actix_web::Result<impl Responder> {
    let booking_id = path.into_inner();
    
    let result = web::block(move || {
        let mut conn = pool.get()?;
        
        let (booking, conference_name) = actions::get_booking_with_conference_name(&mut conn, booking_id)?;
        
        Ok(models::BookingStatusResponse {
            booking_id: booking.booking_id,
            status: booking.status,
            conference_name,
            can_confirm: booking.can_confirm.unwrap_or(false),
            confirmation_deadline: booking.waitlist_confirmation_deadline,
            waitlist_position: booking.waitlist_position,
        })
    })
    .await?
    .map_err(|e: Box<dyn std::error::Error + Send + Sync>| {
        let detail = e.to_string();
        log::error!("Failed to get booking status: {:?}", e);
        
        if let Some(diesel_error) = e.downcast_ref::<diesel::result::Error>() {
            match diesel_error {
                diesel::result::Error::NotFound => {
                    error::InternalError::from_response(
                        e,
                        HttpResponse::NotFound().json(Res { message: "Booking not found".to_string() })
                    )
                }
                _ => error::InternalError::from_response(
                    e,
                    HttpResponse::BadRequest().json(Res { message: detail })
                )
            }
        } else {
            error::InternalError::from_response(
                e,
                HttpResponse::BadRequest().json(Res { message: detail })
            )
        }
    })?;

    Ok(HttpResponse::Ok().json(result))
}

#[post("/confirm")]
async fn confirm_waitlist_booking(
    pool: web::Data<DbPool>,
    form: web::Json<models::ConfirmBookingRequest>
) -> actix_web::Result<impl Responder> {
    let booking_id = form.booking_id;
    let user_id = form.user_id.clone();
    
    let _result = web::block({
        let pool = pool.clone();
        move || {
            let mut conn = pool.get()?;
            
            let (_booking, conference_name) = actions::get_booking_with_conference_name(&mut conn, booking_id)?;
            let conference = actions::get_conference_by_name(&mut conn, &conference_name)?;
            
            let now = Utc::now().naive_utc();
            if conference.start_timestamp <= now {
                return Err("Cannot confirm booking for conference that has already started".into());
            }
            
            let confirmed_booking = actions::confirm_waitlist_booking_secure(&mut conn, booking_id, &user_id)?;
            
            actions::remove_from_overlapping_waitlists(
                &mut conn,
                &user_id,
                conference.start_timestamp,
                conference.end_timestamp,
                conference.conference_id
            )?;
            
            Ok((confirmed_booking, conference_name))
        }
    })
    .await?
    .map_err(|e: Box<dyn std::error::Error + Send + Sync>| {
        let detail = e.to_string();
        log::error!("Failed to confirm waitlist booking: {:?}", e);
        error::InternalError::from_response(e, HttpResponse::BadRequest().json(Res { message: detail }))
    })?;

    Ok(HttpResponse::Ok().json(models::ApiResponse {
        message: "Booking confirmed successfully".to_string(),
    }))
}

#[post("/cancel")]
async fn cancel_booking(
    pool: web::Data<DbPool>,
    queue_service: web::Data<queue::WaitlistQueueService>,
    form: web::Json<models::BookingIdRequest>
) -> actix_web::Result<impl Responder> {
    let booking_id = form.booking_id;
    
    let result = web::block({
        let pool = pool.clone();
        move || {
            let mut conn = pool.get()?;
            
            let (booking, conference_name) = actions::get_booking_with_conference_name(&mut conn, booking_id)?;
            let was_confirmed = booking.status == models::BookingStatus::CONFIRMED;
            
            let canceled_booking = actions::cancel_booking(&mut conn, booking_id)?;
            
            Ok((canceled_booking, conference_name, was_confirmed))
        }
    })
    .await?
    .map_err(|e: Box<dyn std::error::Error + Send + Sync>| {
        let detail = e.to_string();
        log::error!("Failed to cancel booking: {:?}", e);
        error::InternalError::from_response(e, HttpResponse::BadRequest().json(Res { message: detail }))
    })?;

    if result.2 {
        let queue_service_clone = queue_service.clone();
        let conference_name = result.1.clone();
        tokio::spawn(async move {
            if let Err(e) = queue_service_clone.publish_slot_available(&conference_name).await {
                log::error!("Failed to publish slot available message: {:?}", e);
            }
        });
    }

    Ok(HttpResponse::Ok().json(models::ApiResponse {
        message: "Booking canceled successfully".to_string(),
    }))
}

#[get("/conference/{conference_name}/bookings")]
async fn get_conference_bookings(
    pool: web::Data<DbPool>,
    path: web::Path<String>
) -> actix_web::Result<impl Responder> {
    let conference_name = path.into_inner();
    
    let result = web::block(move || {
        let mut conn = pool.get()?;
        
        let conference = actions::get_conference_by_name(&mut conn, &conference_name)?;
        
        use crate::schema::{bookings, users};
        let bookings_with_users: Vec<(models::Booking, Option<String>)> = bookings::table
            .filter(bookings::conference_id.eq(conference.conference_id))
            .left_join(users::table.on(bookings::user_id.eq(users::user_id.nullable())))
            .select((bookings::all_columns, users::user_id.nullable()))
            .load(&mut conn)?;
        
        let booking_responses: Vec<serde_json::Value> = bookings_with_users
            .into_iter()
            .map(|(booking, user_id)| {
                serde_json::json!({
                    "booking_id": booking.booking_id,
                    "user_id": user_id.unwrap_or_default(),
                    "status": booking.status,
                    "created_at": booking.created_at,
                    "waitlist_position": booking.waitlist_position,
                    "can_confirm": booking.can_confirm.unwrap_or(false),
                    "confirmation_deadline": booking.waitlist_confirmation_deadline,
                    "canceled_at": booking.canceled_at
                })
            })
            .collect();
        
        Ok(booking_responses)
    })
    .await?
    .map_err(|e: Box<dyn std::error::Error + Send + Sync>| {
        let detail = e.to_string();
        log::error!("Failed to get conference bookings: {:?}", e);
        
        if let Some(diesel_error) = e.downcast_ref::<diesel::result::Error>() {
            match diesel_error {
                diesel::result::Error::NotFound => {
                    error::InternalError::from_response(
                        e,
                        HttpResponse::NotFound().json(Res { message: "Conference not found".to_string() })
                    )
                }
                _ => error::InternalError::from_response(
                    e,
                    HttpResponse::BadRequest().json(Res { message: detail })
                )
            }
        } else {
            error::InternalError::from_response(
                e,
                HttpResponse::BadRequest().json(Res { message: detail })
            )
        }
    })?;

    Ok(HttpResponse::Ok().json(result))
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenvy::dotenv().ok();
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    // Created outside `HttpServer::new` so all workers share one pool
    let pool = initialize_db_pool();
    
    let mut queue_service = queue::WaitlistQueueService::new(pool.clone());
    queue_service.initialize().await.unwrap();
    
    // Add a small delay to ensure RabbitMQ setup is complete
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    
    let queue_service_clone1 = queue_service.clone();
    tokio::spawn(async move {
        if let Err(e) = queue_service_clone1.start_consuming_expired_confirmations().await {
            log::error!("Error starting expired confirmations consumer: {:?}", e);
        }
    });

    let queue_service_clone2 = queue_service.clone();
    tokio::spawn(async move {
        if let Err(e) = queue_service_clone2.start_consuming_conference_events().await {
            log::error!("Failed to start conference event consumers: {:?}", e);
        }
    });
    
    let queue_service = web::Data::new(queue_service);

    log::info!("starting HTTP server at http://localhost:8080");

    let http = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .app_data(queue_service.clone())
            .wrap(middleware::Logger::default())
            .app_data(web::JsonConfig::default().error_handler(|err, _req| {
                let detail = err.to_string();
                let response = match err {
                    error::JsonPayloadError::ContentType => {
                        HttpResponse::UnsupportedMediaType().body("Unsupported Media Type")
                    }
                    error::JsonPayloadError::Deserialize(ref err) => {
                        HttpResponse::BadRequest().json(Res { message: err.to_string() })
                    }
                    
                    _ => HttpResponse::BadRequest().json(Res { message: detail }),
                };
                error::InternalError::from_response(err, response).into()
            }))
            .service(add_user)
            .service(add_conference)
            .service(book_conference)
            .service(get_booking_status)
            .service(get_conference_bookings)
            .service(confirm_waitlist_booking)
            .service(cancel_booking)
    })
    .bind(("127.0.0.1", 8080)).unwrap()
    .run();

    http.await
}

fn initialize_db_pool() -> DbPool {
    let conn_spec = std::env::var("DATABASE_URL").expect("DATABASE_URL should be set");
    let manager = r2d2::ConnectionManager::<PgConnection>::new(conn_spec);
    r2d2::Pool::builder()
        .build(manager)
        .expect("database URL should be valid path to SQLite DB file")
}
