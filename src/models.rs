use serde::{Deserialize, Serialize};
use crate::schema::{bookings, users, conferences};
use chrono::NaiveDateTime;
use diesel::{deserialize::{self, FromSql}, pg::{Pg, PgValue}, serialize::{self, Output, ToSql}, sql_types::Text, Insertable, Selectable};

#[derive(Debug, Clone, Queryable, Insertable, Serialize, Deserialize, Selectable)]
#[diesel(table_name = users)]
pub struct User {
    pub user_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewUser {
    pub user_id: String,
    pub topics: Vec<String>,
}

#[derive(Debug, Clone, Queryable, Serialize, Deserialize)]
#[diesel(table_name = conferences)]
pub struct Conference {
    pub conference_id: i32,
    pub name: String,
    pub location: String,
    pub start_timestamp: NaiveDateTime,
    pub end_timestamp: NaiveDateTime,
    pub total_slots: i32,
    pub available_slots: i32,
    pub created_at: Option<NaiveDateTime>,
}

#[derive(Debug, Clone, Insertable, Serialize, Deserialize)]
#[diesel(table_name=conferences)]
pub struct NewConferenceInternal {
    pub name: String,
    pub location: String,
    pub start_timestamp: NaiveDateTime,
    pub end_timestamp: NaiveDateTime,
    pub available_slots: i32,
    pub total_slots: i32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NewConference {
    pub name: String,
    pub location: String,
    pub start: String,
    pub end: String,
    pub slots: i32,
    pub topics: Vec<String>
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, AsExpression, FromSqlRow)]
#[diesel(sql_type = crate::schema::sql_types::BookingStatus)]
pub enum BookingStatus {
    CONFIRMED,
    WAITLISTED,
    CANCELED,
    ConfirmationPending,
}

impl ToSql<crate::schema::sql_types::BookingStatus, Pg> for BookingStatus {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        let s = match *self {
            BookingStatus::CONFIRMED => "CONFIRMED",
            BookingStatus::WAITLISTED => "WAITLISTED",
            BookingStatus::CANCELED => "CANCELED",
            BookingStatus::ConfirmationPending => "CONFIRMATION_PENDING",
        };
        <str as ToSql<Text, Pg>>::to_sql(s, out)
    }
}

impl FromSql<crate::schema::sql_types::BookingStatus, Pg> for BookingStatus {
    fn from_sql(bytes: PgValue) -> deserialize::Result<Self> {
        match <String as FromSql<Text, Pg>>::from_sql(bytes)?.as_str() {
            "CONFIRMED" => Ok(BookingStatus::CONFIRMED),
            "WAITLISTED" => Ok(BookingStatus::WAITLISTED),
            "CANCELED" => Ok(BookingStatus::CANCELED),
            "CONFIRMATION_PENDING" => Ok(BookingStatus::ConfirmationPending),
            s => Err(format!("Unrecognized booking status: {}", s).into()),
        }
    }
}

#[derive(Debug, Clone, Queryable, Serialize, Deserialize)]
#[diesel(table_name = bookings)]
pub struct Booking {
    pub booking_id: i32,
    pub conference_id: Option<i32>,
    pub user_id: Option<String>,
    pub status: BookingStatus,
    pub created_at: Option<NaiveDateTime>,
    pub waitlist_confirmation_deadline: Option<NaiveDateTime>,
    pub canceled_at: Option<NaiveDateTime>,
    pub can_confirm: Option<bool>,
    pub waitlist_position: Option<i32>,
}

#[derive(Debug, Clone, Insertable)]
#[diesel(table_name = bookings)]
pub struct NewBooking {
    pub conference_id: i32,
    pub user_id: String,
    pub status: BookingStatus,
    pub waitlist_position: Option<i32>,
    pub can_confirm: Option<bool>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct BookConferenceRequest {
    pub name: String,
    pub user_id: String,
}

#[derive(Debug, Serialize)]
pub struct BookConferenceResponse {
    pub booking_id: i32,
    pub status: BookingStatus,
    pub message: String,
    pub waitlist_position: Option<i32>,
}

#[derive(Debug, Deserialize)]
pub struct BookingIdRequest {
    pub booking_id: i32,
}

#[derive(Debug, Deserialize)]
pub struct ConfirmBookingRequest {
    pub booking_id: i32,
    pub user_id: String,
}

#[derive(Debug, Serialize)]
pub struct BookingStatusResponse {
    pub booking_id: i32,
    pub status: BookingStatus,
    pub conference_name: String,
    pub can_confirm: bool,
    pub confirmation_deadline: Option<NaiveDateTime>,
    pub waitlist_position: Option<i32>,
}

#[derive(Debug, Serialize)]
pub struct ApiResponse {
    pub message: String,
}
