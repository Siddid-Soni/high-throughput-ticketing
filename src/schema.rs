// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::query_builder::QueryId, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "booking_state"))]
    pub struct BookingState;

    #[derive(diesel::query_builder::QueryId, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "seat_status"))]
    pub struct SeatStatus;
}

diesel::table! {
    booking_seats (booking_id, event_seat_id) {
        booking_id -> Uuid,
        event_seat_id -> Int4,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::BookingState;

    bookings (id) {
        id -> Uuid,
        user_id -> Int4,
        event_id -> Int4,
        status -> BookingState,
        total_cents -> Int4,
        #[max_length = 255]
        payment_idempotency_key -> Nullable<Varchar>,
        #[max_length = 255]
        payment_intent_id -> Nullable<Varchar>,
        created_at -> Timestamptz,
        expires_at -> Timestamptz,
        updated_at -> Timestamptz,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::SeatStatus;

    event_seats (id) {
        id -> Int4,
        event_id -> Int4,
        seat_id -> Int4,
        seat_index -> Int4,
        status -> SeatStatus,
        booking_id -> Nullable<Uuid>,
        price_cents -> Int4,
        version -> Int4,
    }
}

diesel::table! {
    events (id) {
        id -> Int4,
        venue_id -> Int4,
        #[max_length = 255]
        name -> Varchar,
        start_time -> Timestamptz,
        #[max_length = 20]
        status -> Varchar,
        created_at -> Timestamptz,
        sale_opens_at -> Timestamptz,
    }
}

diesel::table! {
    payment_inbox (id) {
        id -> Int8,
        #[max_length = 50]
        provider -> Varchar,
        #[max_length = 255]
        provider_event_id -> Varchar,
        #[max_length = 100]
        event_type -> Varchar,
        payload -> Jsonb,
        received_at -> Timestamptz,
        processed_at -> Nullable<Timestamptz>,
        attempts -> Int4,
        last_error -> Nullable<Text>,
    }
}

diesel::table! {
    seats (id) {
        id -> Int4,
        venue_id -> Int4,
        #[max_length = 50]
        section -> Varchar,
        row_number -> Int4,
        seat_number -> Int4,
        #[sql_name = "type"]
        #[max_length = 50]
        type_ -> Varchar,
    }
}

diesel::table! {
    users (id) {
        id -> Int4,
        #[max_length = 255]
        username -> Varchar,
        #[max_length = 255]
        password_hash -> Varchar,
        created_at -> Timestamptz,
    }
}

diesel::table! {
    venues (id) {
        id -> Int4,
        #[max_length = 255]
        name -> Varchar,
        #[max_length = 255]
        location -> Varchar,
        seat_map_config -> Jsonb,
        created_at -> Timestamptz,
    }
}

diesel::joinable!(booking_seats -> bookings (booking_id));
diesel::joinable!(booking_seats -> event_seats (event_seat_id));
diesel::joinable!(bookings -> events (event_id));
diesel::joinable!(bookings -> users (user_id));
diesel::joinable!(event_seats -> bookings (booking_id));
diesel::joinable!(event_seats -> events (event_id));
diesel::joinable!(event_seats -> seats (seat_id));
diesel::joinable!(events -> venues (venue_id));
diesel::joinable!(seats -> venues (venue_id));

diesel::allow_tables_to_appear_in_same_query!(
    booking_seats,
    bookings,
    event_seats,
    events,
    payment_inbox,
    seats,
    users,
    venues,
);
