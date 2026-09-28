use diesel::prelude::*;

use crate::models::{NewSeat, NewVenue, SeatMapConfig, Venue};
use crate::schema::{seats, venues};

// Postgres allows 65,535 bind parameters per statement; NewSeat has 5 columns
const SEAT_INSERT_BATCH: usize = 10_000;

/// Inserts the venue and one `seats` row per seat in `layout`, in one transaction.
/// Returns the venue and the number of seats created.
pub fn create_venue(
    conn: &mut PgConnection,
    new_venue: &NewVenue,
    layout: &SeatMapConfig,
) -> QueryResult<(Venue, usize)> {
    conn.transaction(|conn| {
        let venue: Venue = diesel::insert_into(venues::table)
            .values(new_venue)
            .returning(Venue::as_returning())
            .get_result(conn)?;

        let new_seats: Vec<NewSeat> = layout
            .sections
            .iter()
            .flat_map(|section| {
                (1..=section.rows as i32).flat_map(move |row| {
                    (1..=section.seats_per_row as i32).map(move |seat| NewSeat {
                        venue_id: venue.id,
                        section: &section.name,
                        row_number: row,
                        seat_number: seat,
                        type_: &section.seat_type,
                    })
                })
            })
            .collect();

        for batch in new_seats.chunks(SEAT_INSERT_BATCH) {
            diesel::insert_into(seats::table).values(batch).execute(conn)?;
        }

        Ok((venue, new_seats.len()))
    })
}

/// Returns the venue's distinct section names, or `NotFound` if the venue does not exist.
pub fn venue_sections(conn: &mut PgConnection, venue_id: i32) -> QueryResult<Vec<String>> {
    venues::table.find(venue_id).select(venues::id).first::<i32>(conn)?;
    seats::table
        .filter(seats::venue_id.eq(venue_id))
        .select(seats::section)
        .distinct()
        .load(conn)
}
