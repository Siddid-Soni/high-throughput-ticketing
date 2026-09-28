use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::schema::{booking_seats, bookings, event_seats, events, payment_inbox, seats, users, venues};

#[derive(Debug, Clone, Copy, PartialEq, Eq, diesel_derive_enum::DbEnum, Serialize, Deserialize)]
#[ExistingTypePath = "crate::schema::sql_types::SeatStatus"]
#[DbValueStyle = "SCREAMING_SNAKE_CASE"]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum SeatStatus {
    Available,
    Sold,
    Blocked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, diesel_derive_enum::DbEnum, Serialize, Deserialize)]
#[ExistingTypePath = "crate::schema::sql_types::BookingState"]
#[DbValueStyle = "SCREAMING_SNAKE_CASE"]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum BookingState {
    Pending,
    PaymentPending,
    Confirmed,
    Expired,
    Canceled,
    Refunded,
}

// Not Serialize, so the password hash can never end up in a response
#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = users)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct User {
    pub id: i32,
    pub username: String,
    pub password_hash: String,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = users)]
pub struct NewUser<'a> {
    pub username: &'a str,
    pub password_hash: &'a str,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable, Serialize)]
#[diesel(table_name = venues)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Venue {
    pub id: i32,
    pub name: String,
    pub location: String,
    pub seat_map_config: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

/// Layout of a venue, stored as JSON in `venues.seat_map_config` and expanded
/// into one `seats` row per seat when the venue is created.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeatMapConfig {
    pub sections: Vec<SectionConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SectionConfig {
    pub name: String,
    pub rows: u32,
    pub seats_per_row: u32,
    #[serde(rename = "type", default = "default_seat_type")]
    pub seat_type: String,
}

fn default_seat_type() -> String {
    "standard".to_string()
}

impl SeatMapConfig {
    pub const MAX_SEATS: u64 = 100_000;

    pub fn total_seats(&self) -> u64 {
        self.sections.iter().map(|s| s.rows as u64 * s.seats_per_row as u64).sum()
    }

    /// Returns a message describing the first problem found, if any.
    pub fn validate(&self) -> Result<(), String> {
        if self.sections.is_empty() {
            return Err("seat_map_config must have at least one section".into());
        }
        let mut names = std::collections::HashSet::new();
        for section in &self.sections {
            if section.name.trim().is_empty() {
                return Err("section name must not be empty".into());
            }
            if !names.insert(section.name.as_str()) {
                return Err(format!("section name '{}' is used more than once", section.name));
            }
            if section.rows == 0 || section.seats_per_row == 0 {
                return Err(format!("section '{}' must have at least one row and one seat per row", section.name));
            }
        }
        if self.total_seats() > Self::MAX_SEATS {
            return Err(format!("a venue can have at most {} seats", Self::MAX_SEATS));
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Insertable)]
#[diesel(table_name = venues)]
pub struct NewVenue {
    pub name: String,
    pub location: String,
    pub seat_map_config: serde_json::Value,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable, Associations, Serialize)]
#[diesel(belongs_to(Venue))]
#[diesel(table_name = seats)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Seat {
    pub id: i32,
    pub venue_id: i32,
    pub section: String,
    pub row_number: i32,
    pub seat_number: i32,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = seats)]
pub struct NewSeat<'a> {
    pub venue_id: i32,
    pub section: &'a str,
    pub row_number: i32,
    pub seat_number: i32,
    pub type_: &'a str,
}

// status is one of SCHEDULED, ON_SALE, CLOSED, CANCELED (checked by the database)
#[derive(Debug, Clone, Queryable, Selectable, Identifiable, Associations, Serialize)]
#[diesel(belongs_to(Venue))]
#[diesel(table_name = events)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Event {
    pub id: i32,
    pub venue_id: i32,
    pub name: String,
    pub start_time: DateTime<Utc>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub sale_opens_at: DateTime<Utc>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = events)]
pub struct NewEvent<'a> {
    pub venue_id: i32,
    pub name: &'a str,
    pub start_time: DateTime<Utc>,
    pub sale_opens_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable, Associations, Serialize)]
#[diesel(belongs_to(Event))]
#[diesel(belongs_to(Seat))]
#[diesel(table_name = event_seats)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct EventSeat {
    pub id: i32,
    pub event_id: i32,
    pub seat_id: i32,
    pub seat_index: i32,
    pub status: SeatStatus,
    pub booking_id: Option<Uuid>,
    pub price_cents: i32,
    pub version: i32,
}

/// One seat of an event's seat map: `event_seats` joined with `seats`.
#[derive(Debug, Clone, Queryable, Serialize)]
pub struct EventSeatDetail {
    pub seat_index: i32,
    pub section: String,
    pub row_number: i32,
    pub seat_number: i32,
    #[serde(rename = "type")]
    pub seat_type: String,
    pub price_cents: i32,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = event_seats)]
pub struct NewEventSeat {
    pub event_id: i32,
    pub seat_id: i32,
    pub seat_index: i32,
    pub price_cents: i32,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable, Associations, Serialize)]
#[diesel(belongs_to(User))]
#[diesel(belongs_to(Event))]
#[diesel(table_name = bookings)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct Booking {
    pub id: Uuid,
    pub user_id: i32,
    pub event_id: i32,
    pub status: BookingState,
    pub total_cents: i32,
    pub payment_idempotency_key: Option<String>,
    pub payment_intent_id: Option<String>,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// The id is the reservation id from Redis. Created when payment starts, so
// its status is PAYMENT_PENDING (PENDING is unused: holds live in Redis).
#[derive(Debug, Insertable)]
#[diesel(table_name = bookings)]
pub struct NewBooking<'a> {
    pub id: Uuid,
    pub user_id: i32,
    pub event_id: i32,
    pub status: BookingState,
    pub total_cents: i32,
    pub payment_idempotency_key: &'a str,
    pub expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable, Associations, Insertable, Serialize)]
#[diesel(belongs_to(Booking))]
#[diesel(belongs_to(EventSeat))]
#[diesel(primary_key(booking_id, event_seat_id))]
#[diesel(table_name = booking_seats)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct BookingSeat {
    pub booking_id: Uuid,
    pub event_seat_id: i32,
}

#[derive(Debug, Clone, Queryable, Selectable, Identifiable)]
#[diesel(table_name = payment_inbox)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct PaymentInboxEntry {
    pub id: i64,
    pub provider: String,
    pub provider_event_id: String,
    pub event_type: String,
    pub payload: serde_json::Value,
    pub received_at: DateTime<Utc>,
    pub processed_at: Option<DateTime<Utc>>,
    pub attempts: i32,
    pub last_error: Option<String>,
}

#[derive(Debug, Insertable)]
#[diesel(table_name = payment_inbox)]
pub struct NewPaymentInboxEntry<'a> {
    pub provider: &'a str,
    pub provider_event_id: &'a str,
    pub event_type: &'a str,
    pub payload: &'a serde_json::Value,
}

