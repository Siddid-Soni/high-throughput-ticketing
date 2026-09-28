// Bookings are created when payment starts (never at reserve time: seat holds
// live only in Redis) and end CONFIRMED, REFUNDED or EXPIRED.

use chrono::{DateTime, Utc};
use diesel::prelude::*;
use diesel::QueryResult;
use uuid::Uuid;

use crate::models::{Booking, BookingSeat, BookingState, NewBooking};
use crate::schema::{booking_seats, bookings, event_seats};

pub fn find_booking(conn: &mut PgConnection, booking_id: Uuid) -> QueryResult<Booking> {
    bookings::table.find(booking_id).select(Booking::as_select()).first(conn)
}

/// Records a hold whose payment is starting: a `PAYMENT_PENDING` booking
/// (id = the reservation id) priced from `event_seats`, plus its
/// `booking_seats`, in one transaction. Idempotent: returns the existing
/// booking if there is one. `NotFound` if a seat is not in the event.
pub fn create_payment_pending(
    conn: &mut PgConnection,
    reservation_id: Uuid,
    user_id: i32,
    event_id: i32,
    seat_indexes: &[i32],
    expires_at: DateTime<Utc>,
    idempotency_key: &str,
) -> QueryResult<Booking> {
    conn.transaction(|conn| {
        if let Some(booking) = bookings::table.find(reservation_id).select(Booking::as_select()).first(conn).optional()? {
            return Ok(booking);
        }
        let seats: Vec<(i32, i32)> = event_seats::table
            .filter(event_seats::event_id.eq(event_id))
            .filter(event_seats::seat_index.eq_any(seat_indexes))
            .select((event_seats::id, event_seats::price_cents))
            .load(conn)?;
        if seats.len() != seat_indexes.len() {
            return Err(diesel::result::Error::NotFound);
        }

        let booking = diesel::insert_into(bookings::table)
            .values(&NewBooking {
                id: reservation_id,
                user_id,
                event_id,
                status: BookingState::PaymentPending,
                total_cents: seats.iter().map(|(_, price)| price).sum(),
                payment_idempotency_key: idempotency_key,
                expires_at,
            })
            .returning(Booking::as_returning())
            .get_result(conn)?;
        let rows: Vec<BookingSeat> =
            seats.iter().map(|&(event_seat_id, _)| BookingSeat { booking_id: reservation_id, event_seat_id }).collect();
        diesel::insert_into(booking_seats::table).values(&rows).execute(conn)?;
        Ok(booking)
    })
}

pub fn set_payment_intent(conn: &mut PgConnection, booking_id: Uuid, intent_id: &str) -> QueryResult<()> {
    diesel::update(bookings::table.find(booking_id)).set(bookings::payment_intent_id.eq(intent_id)).execute(conn)?;
    Ok(())
}

/// Sells the booking's seats and confirms it, inside the caller's transaction.
/// The update only takes seats still `AVAILABLE`: if Redis and Postgres ever
/// disagree and a seat is already sold, nothing changes and this returns
/// false (the caller refunds).
pub fn confirm_booking(conn: &mut PgConnection, booking_id: Uuid, event_id: i32, seat_indexes: &[i32]) -> QueryResult<bool> {
    // A savepoint, so a short count undoes only this
    let sold = conn.transaction(|conn| {
        let updated = diesel::update(
            event_seats::table
                .filter(event_seats::event_id.eq(event_id))
                .filter(event_seats::seat_index.eq_any(seat_indexes))
                .filter(event_seats::status.eq(crate::models::SeatStatus::Available)),
        )
        .set((event_seats::status.eq(crate::models::SeatStatus::Sold), event_seats::booking_id.eq(booking_id)))
        .execute(conn)?;
        if updated != seat_indexes.len() {
            return Err(diesel::result::Error::RollbackTransaction);
        }
        Ok(())
    });
    match sold {
        Ok(()) => {
            set_status(conn, booking_id, BookingState::Confirmed)?;
            Ok(true)
        }
        Err(diesel::result::Error::RollbackTransaction) => Ok(false),
        Err(e) => Err(e),
    }
}

pub fn set_status(conn: &mut PgConnection, booking_id: Uuid, status: BookingState) -> QueryResult<()> {
    diesel::update(bookings::table.find(booking_id)).set(bookings::status.eq(status)).execute(conn)?;
    Ok(())
}

/// Marks bookings whose hold expired during payment `EXPIRED`. Holds that
/// never started payment have no booking, so ids without one change nothing.
pub fn expire_payment_pending(conn: &mut PgConnection, ids: &[Uuid]) -> QueryResult<usize> {
    diesel::update(bookings::table.filter(bookings::id.eq_any(ids)).filter(bookings::status.eq(BookingState::PaymentPending)))
        .set(bookings::status.eq(BookingState::Expired))
        .execute(conn)
}

/// Safety net for a cleanup worker that crashed between releasing a hold and
/// marking its booking: expires `PAYMENT_PENDING` bookings whose hold ended
/// before `before`. If a payment did claim one late, the payment worker still
/// confirms it (it only skips `CONFIRMED` and `REFUNDED` bookings).
pub fn expire_stale(conn: &mut PgConnection, before: DateTime<Utc>) -> QueryResult<usize> {
    diesel::update(
        bookings::table.filter(bookings::status.eq(BookingState::PaymentPending)).filter(bookings::expires_at.lt(before)),
    )
    .set(bookings::status.eq(BookingState::Expired))
    .execute(conn)
}

/// Seats in confirmed bookings of an event, per user (for the rebuild)
pub fn paid_seats_by_user(conn: &mut PgConnection, event_id: i32) -> QueryResult<Vec<(i32, i64)>> {
    booking_seats::table
        .inner_join(bookings::table)
        .filter(bookings::event_id.eq(event_id))
        .filter(bookings::status.eq(BookingState::Confirmed))
        .group_by(bookings::user_id)
        .select((bookings::user_id, diesel::dsl::count_star()))
        .load(conn)
}
