use diesel::prelude::*;

use crate::models::{NewUser, User};
use crate::schema::users;

pub fn create_user(conn: &mut PgConnection, new_user: &NewUser) -> QueryResult<User> {
    diesel::insert_into(users::table)
        .values(new_user)
        .returning(User::as_returning())
        .get_result(conn)
}

pub fn find_by_username(conn: &mut PgConnection, username: &str) -> QueryResult<Option<User>> {
    users::table
        .filter(users::username.eq(username))
        .select(User::as_select())
        .first(conn)
        .optional()
}
