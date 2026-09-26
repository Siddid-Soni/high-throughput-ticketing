use diesel::prelude::*;
use chrono::{NaiveDateTime, Utc, Duration};
use crate::models::{self, BookingStatus};

type DbError = Box<dyn std::error::Error + Send + Sync>;

pub fn insert_new_user(conn: &mut PgConnection, nm: &str, topics: &Vec<String>) -> Result<models::User, DbError> {
    use crate::schema::{users::dsl::*, user_interests::dsl::user_id as tuid, user_interests::dsl::{topic, user_interests}};
    
    conn.transaction(|conn| {
        let new_user = models::User {
            user_id: nm.to_owned()
        };

        diesel::insert_into(users).values(&new_user).execute(conn)?;

        let topics = topics.iter().map(|t: &String| {
            (tuid.eq(&new_user.user_id), topic.eq(t))
        }).collect::<Vec<_>>();

        diesel::insert_into(user_interests).values(&topics).execute(conn)?;

        Ok(new_user)
    })
}

pub fn create_new_conference(conn: &mut PgConnection, form: &models::NewConference) -> Result<models::Conference, DbError> {
    use crate::schema::{conferences::dsl::*, conference_topics::dsl::conference_id as cuid, conference_topics::dsl::{topic, conference_topics}};
    
    let start_time = NaiveDateTime::parse_from_str(&form.start, "%Y-%m-%d %H:%M:%S")?;
    let end_time = NaiveDateTime::parse_from_str(&form.end, "%Y-%m-%d %H:%M:%S")?;
    
    if start_time >= end_time {
        return Err("Start timestamp must be before end timestamp".into());
    }
    
    let duration = end_time.signed_duration_since(start_time);
    if duration > Duration::hours(12) {
        return Err("Duration should not exceed 12 hours".into());
    }
    
    if form.slots <= 0 {
        return Err("Available slots must be greater than 0".into());
    }
    
    if form.topics.len() > 10 {
        return Err("Maximum 10 topics allowed".into());
    }
    
    conn.transaction(|conn| {
        let new_conf = models::NewConferenceInternal {
            name: form.name.clone(),
            location: form.location.clone(),
            start_timestamp: start_time,
            end_timestamp: end_time,
            total_slots: form.slots,
            available_slots: form.slots
        };

        let id: i32 = diesel::insert_into(conferences).values(&new_conf).returning(conference_id).get_result(conn)?;

        let topics = form.topics.iter().map(|t| {
            (cuid.eq(id), topic.eq(t))
        }).collect::<Vec<_>>();
        diesel::insert_into(conference_topics).values(&topics).execute(conn)?;

        let created_conference = conferences
            .find(id)
            .first::<models::Conference>(conn)?;

        Ok(created_conference)
    })
}

pub fn get_conference_by_name(conn: &mut PgConnection, conference_name: &str) -> Result<models::Conference, DbError> {
    use crate::schema::conferences::dsl::{conferences, name};
    
    let conference = conferences
        .filter(name.eq(conference_name))
        .first::<models::Conference>(conn)?;
    
    Ok(conference)
}

pub fn get_user_by_id(conn: &mut PgConnection, uid: &str) -> Result<models::User, DbError> {
    use crate::schema::users::dsl::{users, user_id as users_user_id};
    
    let user = users
        .filter(users_user_id.eq(uid))
        .select(models::User::as_select())
        .first::<models::User>(conn)?;
    
    Ok(user)
}

pub fn check_user_has_overlapping_booking(
    conn: &mut PgConnection, 
    user_id: &str, 
    conference_start: NaiveDateTime, 
    conference_end: NaiveDateTime
) -> Result<Option<i32>, DbError> {
    use crate::schema::{bookings, conferences};
    
    let overlapping_booking: Option<i32> = bookings::table
        .inner_join(conferences::table)
        .filter(bookings::user_id.eq(user_id))
        .filter(bookings::status.ne(BookingStatus::CANCELED))
        .filter(
            // Check for overlap: (start1 < end2) AND (start2 < end1)
            conferences::start_timestamp.lt(conference_end)
                .and(conferences::end_timestamp.gt(conference_start))
        )
        .select(bookings::booking_id)
        .first(conn)
        .optional()?;
    
    Ok(overlapping_booking)
}

pub fn check_existing_active_booking(
    conn: &mut PgConnection,
    user_id: &str,
    conference_id: i32
) -> Result<Option<i32>, DbError> {
    use crate::schema::bookings::dsl::{
        bookings, 
        user_id as bookings_user_id, 
        conference_id as bookings_conference_id,
        status,
        booking_id
    };
    
    let existing_booking: Option<i32> = bookings
        .filter(bookings_user_id.eq(user_id))
        .filter(bookings_conference_id.eq(conference_id))
        .filter(status.ne(BookingStatus::CANCELED))
        .select(booking_id)
        .first(conn)
        .optional()?;
    
    Ok(existing_booking)
}

pub fn create_confirmed_booking(
    conn: &mut PgConnection,
    conf_id: i32,
    uid: &str
) -> Result<models::Booking, DbError> {
    use crate::schema::{
        bookings::dsl::{bookings, booking_id},
        conferences::dsl::{conferences, available_slots}
    };
    
    conn.transaction(|conn| {
        diesel::update(conferences.find(conf_id))
            .set(available_slots.eq(available_slots - 1))
            .execute(conn)?;
        
        let new_booking = models::NewBooking {
            conference_id: conf_id,
            user_id: uid.to_string(),
            status: BookingStatus::CONFIRMED,
            waitlist_position: None,
            can_confirm: Some(false),
        };
        
        let new_booking_id = diesel::insert_into(bookings)
            .values(&new_booking)
            .returning(booking_id)
            .get_result::<i32>(conn)?;
        
        let booking = bookings.find(new_booking_id).first::<models::Booking>(conn)?;
        
        Ok(booking)
    })
}

pub fn create_waitlist_booking(
    conn: &mut PgConnection,
    conf_id: i32,
    uid: &str
) -> Result<models::Booking, DbError> {
    use crate::schema::bookings::dsl::{
        bookings,
        conference_id as bookings_conference_id,
        status,
        waitlist_position,
        booking_id
    };
    
    conn.transaction(|conn| {
        let max_position: Option<i32> = bookings
            .filter(bookings_conference_id.eq(conf_id))
            .filter(status.eq(BookingStatus::WAITLISTED))
            .select(diesel::dsl::max(waitlist_position))
            .first(conn)?;
        
        let next_position = max_position.unwrap_or(0) + 1;
        
        let new_booking = models::NewBooking {
            conference_id: conf_id,
            user_id: uid.to_string(),
            status: BookingStatus::WAITLISTED,
            waitlist_position: Some(next_position),
            can_confirm: Some(false),
        };
        
        let new_booking_id = diesel::insert_into(bookings)
            .values(&new_booking)
            .returning(booking_id)
            .get_result::<i32>(conn)?;
        
        let booking = bookings.find(new_booking_id).first::<models::Booking>(conn)?;
        
        Ok(booking)
    })
}

pub fn get_booking_by_id(conn: &mut PgConnection, booking_id: i32) -> Result<models::Booking, DbError> {
    use crate::schema::bookings::dsl::bookings;
    
    let booking = bookings.find(booking_id).first::<models::Booking>(conn)?;
    
    Ok(booking)
}

pub fn get_booking_with_conference_name(
    conn: &mut PgConnection, 
    booking_id: i32
) -> Result<(models::Booking, String), DbError> {
    use crate::schema::{bookings, conferences};
    
    let (booking, conference_name): (models::Booking, String) = bookings::table
        .inner_join(conferences::table)
        .filter(bookings::booking_id.eq(booking_id))
        .select((bookings::all_columns, conferences::name))
        .first(conn)?;
    
    Ok((booking, conference_name))
}

pub fn confirm_waitlist_booking(
    conn: &mut PgConnection,
    booking_id: i32
) -> Result<models::Booking, DbError> {
    use crate::schema::bookings::dsl::{
        bookings, 
        status, 
        waitlist_position, 
        can_confirm, 
        waitlist_confirmation_deadline
    };
    use crate::schema::conferences::dsl::{conferences, available_slots};
    
    conn.transaction(|conn| {
        let booking = bookings.find(booking_id).first::<models::Booking>(conn)?;
        
        if booking.status != BookingStatus::ConfirmationPending {
            return Err("Booking is not in confirmation pending state".into());
        }
        
        if !booking.can_confirm.unwrap_or(false) {
            return Err("Booking cannot be confirmed at this time".into());
        }
        
        let conf_id = booking.conference_id.ok_or("Booking has no conference")?;
        
        diesel::update(conferences.find(conf_id))
            .set(available_slots.eq(available_slots - 1))
            .execute(conn)?;
        
        diesel::update(bookings.find(booking_id))
            .set((
                status.eq(BookingStatus::CONFIRMED),
                waitlist_position.eq::<Option<i32>>(None),
                can_confirm.eq(Some(false)),
                waitlist_confirmation_deadline.eq::<Option<NaiveDateTime>>(None),
            ))
            .execute(conn)?;
        
        let updated_booking = bookings.find(booking_id).first::<models::Booking>(conn)?;
        
        Ok(updated_booking)
    })
}

pub fn confirm_waitlist_booking_secure(
    conn: &mut PgConnection,
    booking_id: i32,
    user_id: &str
) -> Result<models::Booking, DbError> {
    use crate::schema::bookings::dsl::{
        bookings, 
        status, 
        waitlist_position, 
        can_confirm, 
        waitlist_confirmation_deadline
    };
    use crate::schema::conferences::dsl::{conferences, available_slots};
    
    conn.transaction(|conn| {
        let booking = bookings.find(booking_id).first::<models::Booking>(conn)?;
        
        match &booking.user_id {
            Some(booking_user_id) if booking_user_id == user_id => {
            },
            Some(booking_user_id) => {
                return Err(format!("Access denied: booking {} belongs to user '{}', not '{}'", 
                                 booking_id, booking_user_id, user_id).into());
            },
            None => {
                return Err("Booking has no associated user".into());
            }
        }
        
        if booking.status != BookingStatus::ConfirmationPending {
            return Err("Booking is not in confirmation pending state".into());
        }
        
        if !booking.can_confirm.unwrap_or(false) {
            return Err("Booking cannot be confirmed at this time".into());
        }
        
        if let Some(deadline) = booking.waitlist_confirmation_deadline {
            let now = Utc::now().naive_utc();
            if now > deadline {
                return Err("Confirmation deadline has expired".into());
            }
        }
        
        let conf_id = booking.conference_id.ok_or("Booking has no conference")?;
        
        diesel::update(conferences.find(conf_id))
            .set(available_slots.eq(available_slots - 1))
            .execute(conn)?;
        
        diesel::update(bookings.find(booking_id))
            .set((
                status.eq(BookingStatus::CONFIRMED),
                waitlist_position.eq::<Option<i32>>(None),
                can_confirm.eq(Some(false)),
                waitlist_confirmation_deadline.eq::<Option<NaiveDateTime>>(None),
            ))
            .execute(conn)?;
        
        let updated_booking = bookings.find(booking_id).first::<models::Booking>(conn)?;
        
        Ok(updated_booking)
    })
}

pub fn cancel_booking(conn: &mut PgConnection, booking_id: i32) -> Result<models::Booking, DbError> {
    use crate::schema::{
        bookings::dsl::{
            bookings, 
            status, 
            canceled_at, 
            waitlist_position, 
            can_confirm, 
            waitlist_confirmation_deadline
        }, 
        conferences::dsl::{conferences, available_slots}
    };
    
    conn.transaction(|conn| {
        let booking = bookings.find(booking_id).first::<models::Booking>(conn)?;
        
        if booking.status == BookingStatus::CANCELED {
            return Err("Booking is already canceled".into());
        }
        
        if booking.status == BookingStatus::CONFIRMED {
            if let Some(conf_id) = booking.conference_id {
                diesel::update(conferences.find(conf_id))
                    .set(available_slots.eq(available_slots + 1))
                    .execute(conn)?;
            }
        }
        
        diesel::update(bookings.find(booking_id))
            .set((
                status.eq(BookingStatus::CANCELED),
                canceled_at.eq(Some(Utc::now().naive_utc())),
                waitlist_position.eq::<Option<i32>>(None),
                can_confirm.eq(Some(false)),
                waitlist_confirmation_deadline.eq::<Option<NaiveDateTime>>(None),
            ))
            .execute(conn)?;
        
        let updated_booking = bookings.find(booking_id).first::<models::Booking>(conn)?;
        
        Ok(updated_booking)
    })
}

pub fn get_next_waitlist_booking(
    conn: &mut PgConnection,
    conference_id: i32
) -> Result<Option<models::Booking>, DbError> {
    use crate::schema::bookings::dsl::{
        bookings,
        conference_id as bookings_conference_id,
        status,
        can_confirm,
        waitlist_position
    };
    
    let next_booking: Option<models::Booking> = bookings
        .filter(bookings_conference_id.eq(conference_id))
        .filter(status.eq(BookingStatus::WAITLISTED))
        .filter(can_confirm.eq(Some(false)))
        .order(waitlist_position.asc())
        .first(conn)
        .optional()?;
    
    Ok(next_booking)
}

pub fn update_booking_can_confirm(
    conn: &mut PgConnection,
    booking_id: i32,
    can_confirm_flag: bool,
    deadline: Option<NaiveDateTime>
) -> Result<(), DbError> {
    use crate::schema::bookings::dsl::{
        bookings,
        can_confirm,
        waitlist_confirmation_deadline
    };
    
    diesel::update(bookings.find(booking_id))
        .set((
            can_confirm.eq(Some(can_confirm_flag)),
            waitlist_confirmation_deadline.eq(deadline),
        ))
        .execute(conn)?;
    
    Ok(())
}

pub fn remove_from_overlapping_waitlists(
    conn: &mut PgConnection,
    user_id: &str,
    confirmed_conference_start: NaiveDateTime,
    confirmed_conference_end: NaiveDateTime,
    exclude_conference_id: i32
) -> Result<(), DbError> {
    use crate::schema::{bookings, conferences};
    
    conn.transaction(|conn| {
        let overlapping_bookings: Vec<i32> = bookings::table
            .inner_join(conferences::table)
            .filter(bookings::user_id.eq(user_id))
            .filter(bookings::status.eq(BookingStatus::WAITLISTED))
            .filter(conferences::conference_id.ne(exclude_conference_id))
            .filter(
                conferences::start_timestamp.lt(confirmed_conference_end)
                    .and(conferences::end_timestamp.gt(confirmed_conference_start))
            )
            .select(bookings::booking_id)
            .load(conn)?;
        
        if !overlapping_bookings.is_empty() {
            diesel::update(bookings::table)
                .filter(bookings::booking_id.eq_any(overlapping_bookings))
                .set((
                    bookings::status.eq(BookingStatus::CANCELED),
                    bookings::canceled_at.eq(Some(Utc::now().naive_utc())),
                    bookings::waitlist_position.eq::<Option<i32>>(None),
                    bookings::can_confirm.eq(Some(false)),
                    bookings::waitlist_confirmation_deadline.eq::<Option<NaiveDateTime>>(None),
                ))
                .execute(conn)?;
        }
        
        Ok(())
    })
}

pub fn auto_cancel_expired_conferences(conn: &mut PgConnection) -> Result<Vec<String>, DbError> {
    use crate::schema::{bookings, conferences};
    
    let now = Utc::now().naive_utc();
    
    conn.transaction(|conn| {
        let started_conferences: Vec<(i32, String)> = conferences::table
            .filter(conferences::start_timestamp.le(now))
            .select((conferences::conference_id, conferences::name))
            .load(conn)?;
        
        let mut updated_conferences = Vec::new();
        
        for (conf_id, conf_name) in started_conferences {
            let updated_count = diesel::update(bookings::table)
                .filter(bookings::conference_id.eq(conf_id))
                .filter(bookings::status.eq(BookingStatus::WAITLISTED))
                .set((
                    bookings::status.eq(BookingStatus::CANCELED),
                    bookings::canceled_at.eq(Some(now)),
                    bookings::waitlist_position.eq::<Option<i32>>(None),
                    bookings::can_confirm.eq(Some(false)),
                    bookings::waitlist_confirmation_deadline.eq::<Option<NaiveDateTime>>(None),
                ))
                .execute(conn)?;
            
            if updated_count > 0 {
                updated_conferences.push(conf_name);
            }
        }
        
        Ok(updated_conferences)
    })
}

pub fn create_booking_atomic(
    conn: &mut PgConnection,
    conference_id: i32,
    user_id: &str
) -> Result<models::Booking, DbError> {
    use crate::schema::{
        bookings::dsl::{bookings, booking_id as booking_id_col, user_id as bookings_user_id, conference_id as bookings_conference_id, status},
        conferences::dsl::{conferences, available_slots, conference_id as conf_id_col}
    };
    
    conn.transaction(|conn| {
        // Lock the conference row so concurrent bookings serialize on it
        let conference: models::Conference = conferences
            .filter(conf_id_col.eq(conference_id))
            .for_update()
            .first(conn)?;
        
        let existing_booking: Option<i32> = bookings
            .filter(bookings_user_id.eq(user_id))
            .filter(bookings_conference_id.eq(conference_id))
            .filter(status.ne(BookingStatus::CANCELED))
            .select(booking_id_col)
            .for_update()
            .first(conn)
            .optional()?;
        
        if existing_booking.is_some() {
            return Err("User already has an active booking for this conference".into());
        }
        
        let pending_confirmations: i64 = bookings
            .filter(bookings_conference_id.eq(conference_id))
            .filter(status.eq(BookingStatus::ConfirmationPending))
            .count()
            .get_result(conn)?;
        
        let existing_waitlist: i64 = bookings
            .filter(bookings_conference_id.eq(conference_id))
            .filter(status.eq(BookingStatus::WAITLISTED))
            .count()
            .get_result(conn)?;
        
        // Confirm directly only when a slot is free, no confirmation is pending,
        // and nobody is already waitlisted; otherwise join the waitlist to preserve order.
        if conference.available_slots > 0 && pending_confirmations == 0 && existing_waitlist == 0 {
            diesel::update(conferences.filter(conf_id_col.eq(conference_id)))
                .set(available_slots.eq(available_slots - 1))
                .execute(conn)?;
            
            let new_booking = models::NewBooking {
                conference_id,
                user_id: user_id.to_string(),
                status: BookingStatus::CONFIRMED,
                waitlist_position: None,
                can_confirm: Some(false),
            };
            
            let new_booking_id = diesel::insert_into(bookings)
                .values(&new_booking)
                .returning(booking_id_col)
                .get_result::<i32>(conn)?;
            
            let booking = bookings.find(new_booking_id).first::<models::Booking>(conn)?;
            Ok(booking)
        } else {
            create_waitlist_booking_internal(conn, conference_id, user_id)
        }
    })
}

fn create_waitlist_booking_internal(
    conn: &mut PgConnection,
    conf_id: i32,
    uid: &str
) -> Result<models::Booking, DbError> {
    use crate::schema::bookings::dsl::{
        bookings,
        conference_id as bookings_conference_id,
        status,
        waitlist_position,
        booking_id
    };
    
    let max_position: Option<i32> = bookings
        .filter(bookings_conference_id.eq(conf_id))
        .filter(status.eq(BookingStatus::WAITLISTED))
        .select(diesel::dsl::max(waitlist_position))
        .first(conn)?;
    
    let next_position = max_position.unwrap_or(0) + 1;
    
    let new_booking = models::NewBooking {
        conference_id: conf_id,
        user_id: uid.to_string(),
        status: BookingStatus::WAITLISTED,
        waitlist_position: Some(next_position),
        can_confirm: Some(false),
    };
    
    let new_booking_id = diesel::insert_into(bookings)
        .values(&new_booking)
        .returning(booking_id)
        .get_result::<i32>(conn)?;
    
    let booking = bookings.find(new_booking_id).first::<models::Booking>(conn)?;
    Ok(booking)
}
