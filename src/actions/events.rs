use std::collections::HashMap;

use diesel::prelude::*;
use diesel::sql_types::{Array, Int4, Text};
use diesel::QueryResult;

use crate::models::{Event, EventSeatDetail, NewEvent};
use crate::schema::{event_seats, events, seats};

/// Inserts the event and one `event_seats` row per venue seat, in one transaction.
/// `prices` maps each section name to its price in cents; seats in sections
/// without a price are skipped, so callers must check coverage first.
///
/// `seat_index` (the seat's offset in the Redis bitfield) is assigned 0..n-1 in
/// `seats.id` order, which is the order the venue's seats were generated in:
/// section, then row, then seat. Returns the event and the number of seats.
pub fn create_event(
    conn: &mut PgConnection,
    new_event: &NewEvent,
    prices: &HashMap<String, i32>,
) -> QueryResult<(Event, usize)> {
    let (sections, price_cents): (Vec<&str>, Vec<i32>) =
        prices.iter().map(|(section, price)| (section.as_str(), *price)).unzip();

    conn.transaction(|conn| {
        let event = diesel::insert_into(events::table)
            .values(new_event)
            .returning(Event::as_returning())
            .get_result(conn)?;

        // Built in one statement so large venues never round-trip through Rust
        let seat_count = diesel::sql_query(
            "INSERT INTO event_seats (event_id, seat_id, seat_index, price_cents)
             SELECT $1, s.id, (ROW_NUMBER() OVER (ORDER BY s.id) - 1)::int, p.price_cents
             FROM seats s
             JOIN unnest($3::text[], $4::int[]) AS p(section, price_cents) ON p.section = s.section
             WHERE s.venue_id = $2",
        )
        .bind::<Int4, _>(event.id)
        .bind::<Int4, _>(event.venue_id)
        .bind::<Array<Text>, _>(&sections)
        .bind::<Array<Int4>, _>(&price_cents)
        .execute(conn)?;

        Ok((event, seat_count))
    })
}

/// Returns every seat of the event in `seat_index` order, or `NotFound` if the
/// event does not exist. Served by the `(event_id, seat_index)` unique index.
pub fn event_seat_details(conn: &mut PgConnection, event_id: i32) -> QueryResult<Vec<EventSeatDetail>> {
    events::table.find(event_id).select(events::id).first::<i32>(conn)?;
    event_seats::table
        .inner_join(seats::table)
        .filter(event_seats::event_id.eq(event_id))
        .order(event_seats::seat_index.asc())
        .select((
            event_seats::seat_index,
            seats::section,
            seats::row_number,
            seats::seat_number,
            seats::type_,
            event_seats::price_cents,
        ))
        .load(conn)
}

pub fn find_event(conn: &mut PgConnection, event_id: i32) -> QueryResult<Event> {
    events::table.find(event_id).select(Event::as_select()).first(conn)
}

/// Moves a `SCHEDULED` event to `ON_SALE`. Idempotent: returns 0 if it was
/// already on sale (or closed or canceled, which stay as they are).
pub fn open_sale(conn: &mut PgConnection, event_id: i32) -> QueryResult<usize> {
    diesel::update(events::table.find(event_id).filter(events::status.eq("SCHEDULED")))
        .set(events::status.eq("ON_SALE"))
        .execute(conn)
}

/// Closes the sale for good. Idempotent: returns 0 if it was already closed
/// or the event was canceled.
pub fn close_sale(conn: &mut PgConnection, event_id: i32) -> QueryResult<usize> {
    diesel::update(events::table.find(event_id).filter(events::status.eq_any(["SCHEDULED", "ON_SALE"])))
        .set(events::status.eq("CLOSED"))
        .execute(conn)
}

/// Events whose Redis state may be needed: not canceled, and not long over
pub fn live_events(conn: &mut PgConnection) -> QueryResult<Vec<Event>> {
    events::table
        .filter(events::status.ne("CANCELED"))
        .filter(events::start_time.gt(chrono::Utc::now() - chrono::Duration::days(1)))
        .select(Event::as_select())
        .load(conn)
}

/// Seat count, the indexes SOLD or BLOCKED, and how many are SOLD (for the rebuild)
pub fn seat_state(conn: &mut PgConnection, event_id: i32) -> QueryResult<(usize, Vec<i32>, i64)> {
    use crate::models::SeatStatus;
    let seats: Vec<(i32, SeatStatus)> = event_seats::table
        .filter(event_seats::event_id.eq(event_id))
        .select((event_seats::seat_index, event_seats::status))
        .load(conn)?;
    let taken = seats.iter().filter(|(_, status)| *status != SeatStatus::Available).map(|(index, _)| *index).collect();
    let sold = seats.iter().filter(|(_, status)| *status == SeatStatus::Sold).count() as i64;
    Ok((seats.len(), taken, sold))
}
