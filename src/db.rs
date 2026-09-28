use diesel::{r2d2, PgConnection};

use crate::config::Config;

pub type DbPool = r2d2::Pool<r2d2::ConnectionManager<PgConnection>>;

pub fn init_pool(config: &Config) -> DbPool {
    let manager = r2d2::ConnectionManager::<PgConnection>::new(&config.database_url);
    let pool = r2d2::Pool::builder()
        .max_size(config.db_pool_size)
        .build(manager)
        .expect("DATABASE_URL should point to a reachable PostgreSQL database");
    log::info!("Database pool size: {}", config.db_pool_size);
    pool
}
