use actix_web::{middleware, web, App, HttpServer};

use std::sync::Arc;

use conference::{
    api, cache,
    config::Config,
    db,
    payment,
    waiting::{KafkaQueue, SharedQueue},
};

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    env_logger::init_from_env(env_logger::Env::new().default_filter_or("info"));

    let config = Config::from_env();

    // Created outside `HttpServer::new` so all workers share one pool
    let pool = db::init_pool(&config);
    let redis = cache::connect(&config).await;
    let queue: SharedQueue = Arc::new(KafkaQueue::connect(&config).await.expect("Kafka should be reachable at KAFKA_BROKERS"));
    let queue = web::Data::new(queue);
    let provider = web::Data::new(payment::from_config(&config, pool.clone()));
    let config = web::Data::new(config);

    log::info!("starting HTTP server at http://localhost:8080");

    HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(pool.clone()))
            .app_data(web::Data::new(redis.clone()))
            .app_data(queue.clone())
            .app_data(provider.clone())
            .app_data(config.clone())
            .app_data(api::json_config())
            .wrap(middleware::Logger::default())
            .configure(api::configure)
    })
    .bind(("127.0.0.1", 8080))?
    .run()
    .await
}
