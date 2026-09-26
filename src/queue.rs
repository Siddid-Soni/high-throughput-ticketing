use amqprs::{
    callbacks::{DefaultChannelCallback, DefaultConnectionCallback}, 
    channel::{BasicConsumeArguments, BasicPublishArguments, Channel, QueueBindArguments, QueueDeclareArguments, BasicAckArguments, BasicNackArguments}, 
    connection::{Connection, OpenConnectionArguments}, 
    consumer::AsyncConsumer, 
    BasicProperties, 
    FieldTable,
    Deliver,
};
use diesel::{
    prelude::*,
    r2d2::{ConnectionManager, Pool},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use chrono::{DateTime, Utc, Duration};
use log::{error, info, warn};
use uuid::Uuid;
use crate::models::{Booking, Conference, BookingStatus};
use crate::schema::{bookings, conferences};
use tokio::sync::Mutex;

const CONFIRMATION_TIMEOUT_SECONDS: i64 = 20;
const CONFIRMATION_TIMEOUT_MS: u32 = (CONFIRMATION_TIMEOUT_SECONDS * 1000) as u32;

type DbPool = Pool<ConnectionManager<PgConnection>>;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

#[derive(Debug, Serialize, Deserialize)]
struct SlotAvailableMessage {
    booking_id: i32,
    user_id: String,
    conference_name: String,
    confirmation_deadline: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
struct ConfirmationExpirationMessage {
    booking_id: i32,
    expiration_time: DateTime<Utc>,
    conference_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct ConferenceStartMessage {
    conference_name: String,
    start_time: DateTime<Utc>,
}

struct ExpiredConfirmationConsumer {
    db_pool: DbPool,
}

impl ExpiredConfirmationConsumer {
    fn new(db_pool: DbPool) -> Self {
        Self { db_pool }
    }
}

struct ConferenceStartConsumer {
    db_pool: DbPool,
    waitlist_queue_prefix: String,
}

impl ConferenceStartConsumer {
    fn new(db_pool: DbPool, waitlist_queue_prefix: String) -> Self {
        Self { 
            db_pool,
            waitlist_queue_prefix 
        }
    }
}

#[async_trait::async_trait]
impl AsyncConsumer for ExpiredConfirmationConsumer {
    async fn consume(
        &mut self,
        channel: &Channel,
        deliver: Deliver,
        _basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        info!("Processing expired confirmation message");
        
        let result = self.handle_expired_confirmation(channel, deliver, content).await;
        if let Err(e) = result {
            error!("Failed to process expired confirmation: {:?}", e);
        }
    }
}

#[async_trait::async_trait]
impl AsyncConsumer for ConferenceStartConsumer {
    async fn consume(
        &mut self,
        channel: &Channel,
        deliver: Deliver,
        _basic_properties: BasicProperties,
        content: Vec<u8>,
    ) {
        info!("Processing conference start event");
        
        let result = self.handle_conference_start(channel, deliver, content).await;
        if let Err(e) = result {
            error!("Failed to process conference start event: {:?}", e);
        }
    }
}

impl ExpiredConfirmationConsumer {
    async fn handle_expired_confirmation(&mut self, channel: &Channel, deliver: Deliver, content: Vec<u8>) -> Result<()> {
        match serde_json::from_slice::<ConfirmationExpirationMessage>(&content) {
            Ok(message) => {
                info!("Confirmation expired for booking {} from conference {}", message.booking_id, message.conference_name);
                
                match self.db_pool.get() {
                    Ok(mut conn) => {
                        match self.move_booking_to_waitlist_end(&mut conn, message.booking_id, &message.conference_name).await {
                            Ok(true) => {
                                info!("Moved booking {} back to waitlist for conference {}", message.booking_id, message.conference_name);
                                
                                if let Err(e) = self.promote_next_waitlisted_person(&mut conn, &message.conference_name, channel).await {
                                    error!("Failed to promote next waitlisted person for '{}': {:?}", message.conference_name, e);
                                }
                                
                                if let Err(e) = channel.basic_ack(BasicAckArguments::new(deliver.delivery_tag(), false)).await {
                                    error!("Error acknowledging message: {:?}", e);
                                }
                            },
                            Ok(false) => {
                                info!("Booking {} was not in confirmation pending state", message.booking_id);
                                if let Err(e) = channel.basic_ack(BasicAckArguments::new(deliver.delivery_tag(), false)).await {
                                    error!("Error acknowledging message: {:?}", e);
                                }
                            },
                            Err(e) => {
                                error!("Error processing expired confirmation: {:?}", e);
                                // Transient failure: requeue
                                if let Err(e) = channel.basic_nack(BasicNackArguments::new(deliver.delivery_tag(), false, true)).await {
                                    error!("Error rejecting message: {:?}", e);
                                }
                                return Err(e);
                            }
                        }
                    },
                    Err(e) => {
                        error!("Database connection error: {:?}", e);
                        // Transient failure: requeue
                        if let Err(e) = channel.basic_nack(BasicNackArguments::new(deliver.delivery_tag(), false, true)).await {
                            error!("Error rejecting message: {:?}", e);
                        }
                        return Err(e.into());
                    }
                }
            },
            Err(e) => {
                error!("Error deserializing expired confirmation message: {:?}", e);
                // Malformed message: drop without requeue
                if let Err(e) = channel.basic_nack(BasicNackArguments::new(deliver.delivery_tag(), false, false)).await {
                    error!("Error rejecting message: {:?}", e);
                }
                return Err(e.into());
            }
        }
        
        Ok(())
    }

    async fn move_booking_to_waitlist_end(&self, conn: &mut PgConnection, booking_id: i32, conference_name: &str) -> Result<bool> {
        use crate::actions::{get_conference_by_name};
        
        let conference = get_conference_by_name(conn, conference_name)?;
        
        let max_position: Option<i32> = bookings::table
            .filter(bookings::conference_id.eq(conference.conference_id))
            .filter(bookings::status.eq(BookingStatus::WAITLISTED))
            .select(diesel::dsl::max(bookings::waitlist_position))
            .first(conn)?;
            
        let new_position = max_position.unwrap_or(0) + 1;
            
        let updated = diesel::update(bookings::table)
            .filter(bookings::booking_id.eq(booking_id))
            .filter(bookings::status.eq(BookingStatus::ConfirmationPending))
            .set((
                bookings::status.eq(BookingStatus::WAITLISTED),
                bookings::can_confirm.eq(false),
                bookings::waitlist_confirmation_deadline.eq(None::<chrono::NaiveDateTime>),
                bookings::waitlist_position.eq(new_position),
            ))
            .execute(conn)?;
            
        Ok(updated > 0)
    }
    
    async fn promote_next_waitlisted_person(&self, conn: &mut PgConnection, conference_name: &str, channel: &Channel) -> Result<()> {
        use crate::actions::get_conference_by_name;
        
        let conference = get_conference_by_name(conn, conference_name)?;
        
        if conference.available_slots <= 0 {
            info!("No available slots in conference '{}' - skipping auto-promotion", conference_name);
            return Ok(());
        }
        
        let next_waitlisted = bookings::table
            .filter(bookings::conference_id.eq(conference.conference_id))
            .filter(bookings::status.eq(BookingStatus::WAITLISTED))
            .order_by(bookings::waitlist_position.asc())
            .first::<Booking>(conn)
            .optional()?;
        
        if let Some(booking) = next_waitlisted {
            let deadline = Utc::now() + Duration::seconds(CONFIRMATION_TIMEOUT_SECONDS);
            
            diesel::update(bookings::table)
                .filter(bookings::booking_id.eq(booking.booking_id))
                .set((
                    bookings::waitlist_confirmation_deadline.eq(Some(deadline.naive_utc())),
                    bookings::can_confirm.eq(true),
                    bookings::status.eq(BookingStatus::ConfirmationPending),
                    bookings::waitlist_position.eq(None::<i32>),
                ))
                .execute(conn)?;
            
            let expiration_msg = ConfirmationExpirationMessage {
                booking_id: booking.booking_id,
                expiration_time: deadline,
                conference_name: conference_name.to_string(),
            };
            
            let serialized = serde_json::to_string(&expiration_msg)?;
            let content = serialized.as_bytes().to_vec();
            
            let properties = BasicProperties::default()
                .with_delivery_mode(2) // persistent
                .with_expiration(&CONFIRMATION_TIMEOUT_MS.to_string())
                .finish();
            
            let args = BasicPublishArguments::new("", "confirmation.timer");
            
            channel
                .basic_publish(
                    properties,
                    content,
                    args,
                )
                .await?;
            
            info!("Auto-promoted booking {} from waitlist for conference '{}' (slots available: {}). Confirmation expires in {} seconds at {}", 
                  booking.booking_id, conference_name, conference.available_slots, CONFIRMATION_TIMEOUT_SECONDS, deadline);
        } else {
            info!("No more waitlisted bookings for conference '{}' - waitlist exhausted", conference_name);
        }
        
        Ok(())
    }
}

impl ConferenceStartConsumer {
    async fn handle_conference_start(&mut self, channel: &Channel, deliver: Deliver, content: Vec<u8>) -> Result<()> {
        match serde_json::from_slice::<ConferenceStartMessage>(&content) {
            Ok(message) => {
                info!("Conference '{}' has started at {}", message.conference_name, message.start_time);
                
                match self.db_pool.get() {
                    Ok(mut conn) => {
                        match self.process_conference_start(&mut conn, &message.conference_name, channel).await {
                            Ok(cancelled_count) => {
                                if cancelled_count > 0 {
                                    info!("Cancelled {} waitlisted bookings and cleaned up queue for conference '{}'", 
                                          cancelled_count, message.conference_name);
                                }
                                
                                if let Err(e) = channel.basic_ack(BasicAckArguments::new(deliver.delivery_tag(), false)).await {
                                    error!("Error acknowledging message: {:?}", e);
                                }
                            },
                            Err(e) => {
                                error!("Error processing conference start: {:?}", e);
                                // Transient failure: requeue
                                if let Err(e) = channel.basic_nack(BasicNackArguments::new(deliver.delivery_tag(), false, true)).await {
                                    error!("Error rejecting message: {:?}", e);
                                }
                                return Err(e);
                            }
                        }
                    },
                    Err(e) => {
                        error!("Database connection error: {:?}", e);
                        // Transient failure: requeue
                        if let Err(e) = channel.basic_nack(BasicNackArguments::new(deliver.delivery_tag(), false, true)).await {
                            error!("Error rejecting message: {:?}", e);
                        }
                        return Err(e.into());
                    }
                }
            },
            Err(e) => {
                error!("Error deserializing conference start message: {:?}", e);
                // Malformed message: drop without requeue
                if let Err(e) = channel.basic_nack(BasicNackArguments::new(deliver.delivery_tag(), false, false)).await {
                    error!("Error rejecting message: {:?}", e);
                }
                return Err(e.into());
            }
        }
        
        Ok(())
    }

    async fn process_conference_start(&self, conn: &mut PgConnection, conference_name: &str, channel: &Channel) -> Result<i32> {
        use crate::actions::get_conference_by_name;
        
        let conference = get_conference_by_name(conn, conference_name)?;
        
        let cancelled_count = diesel::update(bookings::table)
            .filter(bookings::conference_id.eq(conference.conference_id))
            .filter(bookings::status.eq_any(vec![BookingStatus::WAITLISTED, BookingStatus::ConfirmationPending]))
            .set((
                bookings::status.eq(BookingStatus::CANCELED),
                bookings::canceled_at.eq(Some(Utc::now().naive_utc())),
                bookings::waitlist_position.eq::<Option<i32>>(None),
                bookings::can_confirm.eq(Some(false)),
                bookings::waitlist_confirmation_deadline.eq::<Option<chrono::NaiveDateTime>>(None),
            ))
            .execute(conn)?;
        
        let queue_name = format!("{}{}.waitlist", self.waitlist_queue_prefix, conference_name);
        
        match channel.queue_delete(
            amqprs::channel::QueueDeleteArguments::new(&queue_name)
                .if_empty(false)
                .if_unused(false)
                .finish()
        ).await {
            Ok(_) => {
                info!("Deleted queue: {}", queue_name);
            },
            Err(e) => {
                // The queue may not exist; not an error
                warn!("Could not delete queue {}: {:?}", queue_name, e);
            }
        }
        
        Ok(cancelled_count as i32)
    }
}

pub struct WaitlistQueueService {
    db_pool: DbPool,
    connection: Option<Arc<Connection>>,
    channel_pool: Arc<Mutex<Vec<Channel>>>,
    max_channels: usize,
    conference_exchange: String,
    booking_exchange: String,
    waitlist_queue_prefix: String,
    confirmation_timer_queue: String,
    dead_letter_exchange: String,
    dead_letter_queue: String,
    conference_start_queue: String,
}

impl WaitlistQueueService {
    pub fn new(db_pool: DbPool) -> Self {
        Self {
            db_pool,
            connection: None,
            channel_pool: Arc::new(Mutex::new(Vec::new())),
            max_channels: 10,
            conference_exchange: "conference.events".to_string(),
            booking_exchange: "booking.events".to_string(),
            waitlist_queue_prefix: "conference.".to_string(),
            confirmation_timer_queue: "confirmation.timer".to_string(),
            dead_letter_exchange: "dead.letter.exchange".to_string(),
            dead_letter_queue: "confirmation.expired".to_string(),
            conference_start_queue: "conference.starts".to_string(),
        }
    }
    
    pub async fn initialize(&mut self) -> Result<()> {
        info!("Connecting to RabbitMQ");
        
        let connection = Connection::open(&OpenConnectionArguments::new(
            "localhost", 
            5672,
            "guest", 
            "guest",
        )).await?;
        
        connection
            .register_callback(DefaultConnectionCallback)
            .await?;
        
        let setup_channel = connection.open_channel(None).await?;
        setup_channel
            .register_callback(DefaultChannelCallback)
            .await?;
        
        setup_channel
            .exchange_declare(
                amqprs::channel::ExchangeDeclareArguments::new(
                    &self.conference_exchange,
                    "topic",
                )
                .durable(true)
                .finish(),
            )
            .await?;
        
        setup_channel
            .exchange_declare(
                amqprs::channel::ExchangeDeclareArguments::new(
                    &self.booking_exchange,
                    "direct",
                )
                .durable(true)
                .finish(),
            )
            .await?;
            
        setup_channel
            .exchange_declare(
                amqprs::channel::ExchangeDeclareArguments::new(
                    &self.dead_letter_exchange,
                    "direct",
                )
                .durable(true)
                .finish(),
            )
            .await?;
            
        setup_channel
            .queue_declare(
                QueueDeclareArguments::new(&self.dead_letter_queue)
                    .durable(true)
                    .finish(),
            )
            .await?;
            
        setup_channel
            .queue_bind(
                QueueBindArguments::new(
                    &self.dead_letter_queue,
                    &self.dead_letter_exchange,
                    "confirmation.expired",
                )
                .finish(),
            )
            .await?;
            
        let mut args = FieldTable::new();
        args.insert(
            "x-dead-letter-exchange".try_into()?,
            self.dead_letter_exchange.clone().into()
        );
        args.insert(
            "x-dead-letter-routing-key".try_into()?,
            "confirmation.expired".into()
        );
        
        setup_channel
            .queue_declare(
                QueueDeclareArguments::new(&self.confirmation_timer_queue)
                    .durable(true)
                    .arguments(args)
                    .finish(),
            )
            .await?;
            
        setup_channel
            .queue_declare(
                QueueDeclareArguments::new(&self.conference_start_queue)
                    .durable(true)
                    .finish(),
            )
            .await?;
            
        setup_channel
            .queue_bind(
                QueueBindArguments::new(
                    &self.conference_start_queue,
                    &self.conference_exchange,
                    "conference.start",
                )
                .finish(),
            )
            .await?;
        
        self.connection = Some(Arc::new(connection));
        
        let _ = setup_channel.close().await;
        
        info!("Connected to RabbitMQ and declared queues");
        
        Ok(())
    }
    
    async fn get_fresh_channel(&self) -> Result<Channel> {
        if let Some(connection) = &self.connection {
            let channel = connection.open_channel(None).await?;
            channel.register_callback(DefaultChannelCallback).await?;
            
            // Small delay to ensure channel is fully ready
            tokio::time::sleep(tokio::time::Duration::from_millis(5)).await;
            
            Ok(channel)
        } else {
            Err("RabbitMQ connection not initialized".into())
        }
    }
    
    async fn safe_queue_operation<F, Fut>(&self, operation: F) -> Result<()>
    where
        F: Fn() -> Fut + Clone,
        Fut: std::future::Future<Output = Result<()>>,
    {
        let max_retries = 2;
        let mut delay_ms = 25;
        
        for attempt in 1..=max_retries {
            match operation().await {
                Ok(_) => return Ok(()),
                Err(e) => {
                    if attempt < max_retries {
                        warn!("Queue operation failed (attempt {}/{}), retrying: {:?}", attempt, max_retries, e);
                        tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                        delay_ms *= 2;
                    } else {
                        error!("Queue operation failed after {} attempts, giving up: {:?}", max_retries, e);
                        // Queue failures are logged, not propagated, so they never fail a booking
                        return Ok(());
                    }
                }
            }
        }
        
        Ok(())
    }
    
    pub async fn add_to_waitlist(&self, booking: &Booking, conference_name: &str) -> Result<()> {
        let booking_clone = booking.clone();
        let conference_name_clone = conference_name.to_string();
        
        let operation = move || {
            let booking = booking_clone.clone();
            let conference_name = conference_name_clone.clone();
            let service = self.clone();
            
            async move {
                let channel = service.get_fresh_channel().await?;
                
                let queue_name = format!("{}{}.waitlist", service.waitlist_queue_prefix, conference_name);
                
                channel
                    .queue_declare(
                        QueueDeclareArguments::new(&queue_name)
                            .durable(true)
                            .finish(),
                    )
                    .await?;
                
                let message = SlotAvailableMessage {
                    booking_id: booking.booking_id,
                    user_id: booking.user_id.clone().unwrap_or_default(),
                    conference_name: conference_name.clone(),
                    confirmation_deadline: Utc::now(),
                };
                
                let serialized = serde_json::to_string(&message)?;
                let content = serialized.as_bytes().to_vec();
                
                let properties = BasicProperties::default()
                    .with_delivery_mode(2) // persistent
                    .finish();
                
                let args = BasicPublishArguments::new("", &queue_name);
                
                channel
                    .basic_publish(
                        properties,
                        content,
                        args,
                    )
                    .await?;
                
                let _ = channel.close().await;
                
                info!("Added booking {} to waitlist for conference {}", booking.booking_id, conference_name);
                Ok(())
            }
        };
        
        self.safe_queue_operation(operation).await
    }
    
    pub async fn publish_slot_available(&self, conference_name: &str) -> Result<()> {
        let conference_name_clone = conference_name.to_string();
        
        let operation = move || {
            let conference_name = conference_name_clone.clone();
            let service = self.clone();
            
            async move {
                let channel = service.get_fresh_channel().await?;
                
                let mut conn = service.db_pool.get()?;
                
                let conference = conferences::table
                    .filter(conferences::name.eq(&conference_name))
                    .first::<Conference>(&mut conn)
                    .optional()?;
                
                let conference = match conference {
                    Some(conf) => conf,
                    None => {
                        info!("Conference '{}' not found", conference_name);
                        return Ok(());
                    }
                };
                
                if conference.available_slots <= 0 {
                    info!("No available slots in conference '{}' - skipping waitlist promotion", conference_name);
                    return Ok(());
                }
                
                let next_waitlisted = bookings::table
                    .filter(bookings::status.eq(BookingStatus::WAITLISTED))
                    .filter(bookings::conference_id.eq(conference.conference_id))
                    .order_by(bookings::waitlist_position.asc())
                    .first::<Booking>(&mut conn)
                    .optional()?;
                
                if let Some(booking) = next_waitlisted {
                    let deadline = Utc::now() + Duration::seconds(CONFIRMATION_TIMEOUT_SECONDS);
                    
                    diesel::update(bookings::table)
                        .filter(bookings::booking_id.eq(booking.booking_id))
                        .set((
                            bookings::waitlist_confirmation_deadline.eq(Some(deadline.naive_utc())),
                            bookings::can_confirm.eq(true),
                            bookings::status.eq(BookingStatus::ConfirmationPending),
                            bookings::waitlist_position.eq(None::<i32>),
                        ))
                        .execute(&mut conn)?;
                    
                    let mut args = FieldTable::new();
                    args.insert(
                        "x-dead-letter-exchange".try_into()?,
                        service.dead_letter_exchange.clone().into()
                    );
                    args.insert(
                        "x-dead-letter-routing-key".try_into()?,
                        "confirmation.expired".into()
                    );
                    
                    channel
                        .queue_declare(
                            QueueDeclareArguments::new(&service.confirmation_timer_queue)
                                .durable(true)
                                .arguments(args)
                                .finish(),
                        )
                        .await?;
                    
                    let expiration_msg = ConfirmationExpirationMessage {
                        booking_id: booking.booking_id,
                        expiration_time: deadline,
                        conference_name: conference_name.clone(),
                    };
                    
                    let serialized = serde_json::to_string(&expiration_msg)?;
                    let content = serialized.as_bytes().to_vec();
                    
                    let properties = BasicProperties::default()
                        .with_delivery_mode(2) // persistent
                        .with_expiration(&CONFIRMATION_TIMEOUT_MS.to_string())
                        .finish();
                    
                    let args = BasicPublishArguments::new("", &service.confirmation_timer_queue);
                    
                    channel
                        .basic_publish(
                            properties,
                            content,
                            args,
                        )
                        .await?;
                    
                    info!(
                        "Promoted booking {} from waitlist for conference '{}' (slots available: {}). Confirmation expires in {} seconds at {}", 
                        booking.booking_id, conference_name, conference.available_slots, CONFIRMATION_TIMEOUT_SECONDS, deadline
                    );
                } else {
                    info!("No waitlisted bookings found for conference '{}'", conference_name);
                }
                
                let _ = channel.close().await;
                Ok(())
            }
        };
        
        self.safe_queue_operation(operation).await
    }
    
    pub async fn start_consuming_expired_confirmations(&self) -> Result<()> {
        if let Some(connection) = &self.connection {
            info!("Starting dead letter queue consumer for expired confirmations");
            info!("Dead letter queue name: {}", self.dead_letter_queue);
            
            let channel = connection.open_channel(None).await?;
            channel.register_callback(DefaultChannelCallback).await?;
            
            let db_pool = self.db_pool.clone();
            let dead_letter_queue = self.dead_letter_queue.clone();
            
            let consumer_tag = format!("expired_confirmation_consumer_{}", Uuid::new_v4());
            let args = BasicConsumeArguments::new(&dead_letter_queue, &consumer_tag)
                .manual_ack(true)
                .finish();
            
            let consumer = ExpiredConfirmationConsumer::new(db_pool);
            
            tokio::spawn(async move {
                info!("Started consuming expired confirmation messages");
                
                match channel.basic_consume(consumer, args).await {
                    Ok(_) => {
                        info!("Dead letter queue consumer started successfully");
                        loop {
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        }
                    },
                    Err(e) => {
                        error!("Failed to start dead letter queue consumer: {:?}", e);
                    }
                }
            });
        } else {
            return Err("RabbitMQ connection not initialized".into());
        }
        
        Ok(())
    }

    pub async fn start_consuming_conference_events(&self) -> Result<()> {
        if let Some(connection) = &self.connection {
            info!("Starting conference start event consumer");
            
            let channel = connection.open_channel(None).await?;
            channel.register_callback(DefaultChannelCallback).await?;
            
            let db_pool = self.db_pool.clone();
            let waitlist_queue_prefix = self.waitlist_queue_prefix.clone();
            let conference_start_queue = self.conference_start_queue.clone();
            
            tokio::spawn(async move {
                info!("Started conference start event consumer on queue: {}", conference_start_queue);
                
                let consumer_tag = format!("conference_start_consumer_{}", Uuid::new_v4());
                let args = BasicConsumeArguments::new(&conference_start_queue, &consumer_tag)
                    .manual_ack(true)
                    .finish();
                
                let consumer = ConferenceStartConsumer::new(db_pool, waitlist_queue_prefix);
                
                match channel.basic_consume(consumer, args).await {
                    Ok(_) => {
                        info!("Conference start event consumer started successfully");
                        loop {
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        }
                    },
                    Err(e) => {
                        error!("Failed to start conference start event consumer: {:?}", e);
                    }
                }
            });
        } else {
            return Err("RabbitMQ connection not initialized".into());
        }
        
        Ok(())
    }

    pub async fn add_to_waitlist_by_booking_id(&self, booking_id: i32, conference_name: &str) -> Result<()> {
        let mut conn = self.db_pool.get()?;
        
        let booking = bookings::table
            .find(booking_id)
            .first::<Booking>(&mut conn)?;
        
        self.add_to_waitlist(&booking, conference_name).await
    }

    pub async fn schedule_conference_start_event(&self, conference_name: &str, start_time: DateTime<Utc>) -> Result<()> {
        let conference_name_clone = conference_name.to_string();
        let start_time_clone = start_time;
        
        let operation = move || {
            let conference_name = conference_name_clone.clone();
            let start_time = start_time_clone;
            let service = self.clone();
            
            async move {
                let now = Utc::now();
                let delay_seconds = (start_time - now).num_seconds();
                
                if delay_seconds > 0 {
                    info!("Scheduled conference start event for '{}' at {} (in {} seconds)", 
                          conference_name, start_time, delay_seconds);
                    
                    let channel = service.get_fresh_channel().await?;
                    
                    channel
                        .queue_declare(
                            QueueDeclareArguments::new(&service.conference_start_queue)
                                .durable(true)
                                .finish(),
                        )
                        .await?;
                    
                    let timer_queue_name = "conference.start.timer";
                    let mut args = FieldTable::new();
                    args.insert(
                        "x-dead-letter-exchange".try_into()?,
                        "".into() // Route to default exchange
                    );
                    args.insert(
                        "x-dead-letter-routing-key".try_into()?,
                        service.conference_start_queue.clone().into()
                    );
                    
                    channel
                        .queue_declare(
                            QueueDeclareArguments::new(timer_queue_name)
                                .durable(true)
                                .arguments(args)
                                .finish(),
                        )
                        .await?;
                    
                    let start_msg = ConferenceStartMessage {
                        conference_name: conference_name.clone(),
                        start_time,
                    };
                    
                    let serialized = serde_json::to_string(&start_msg)?;
                    let content = serialized.as_bytes().to_vec();
                    
                    let ttl_ms = (delay_seconds * 1000).max(1);
                    let properties = BasicProperties::default()
                        .with_delivery_mode(2) // persistent
                        .with_expiration(&ttl_ms.to_string())
                        .finish();
                    
                    let args = BasicPublishArguments::new("", timer_queue_name);
                    
                    channel.basic_publish(properties, content, args).await?;
                    
                    info!("Published conference start timer message for '{}' with TTL {}ms to shared queue", conference_name, ttl_ms);
                    info!("Message will be dead-lettered to '{}' queue when TTL expires", service.conference_start_queue);
                    
                    let _ = channel.close().await;
                } else {
                    // Conference start time has passed, trigger immediately
                    let start_msg = ConferenceStartMessage {
                        conference_name: conference_name.clone(),
                        start_time,
                    };
                    
                    let channel = service.get_fresh_channel().await?;
                    let serialized = serde_json::to_string(&start_msg)?;
                    let content = serialized.as_bytes().to_vec();
                    
                    let properties = BasicProperties::default()
                        .with_delivery_mode(2) // persistent
                        .finish();
                    
                    let args = BasicPublishArguments::new("", &service.conference_start_queue);
                    
                    channel.basic_publish(properties, content, args).await?;
                    
                    info!("Conference '{}' start time has passed - triggering cleanup immediately", conference_name);
                    
                    let _ = channel.close().await;
                }
                
                Ok(())
            }
        };
        
        self.safe_queue_operation(operation).await
    }
}

impl Clone for WaitlistQueueService {
    fn clone(&self) -> Self {
        Self {
            db_pool: self.db_pool.clone(),
            connection: self.connection.clone(),
            channel_pool: self.channel_pool.clone(),
            max_channels: self.max_channels,
            conference_exchange: self.conference_exchange.clone(),
            booking_exchange: self.booking_exchange.clone(),
            waitlist_queue_prefix: self.waitlist_queue_prefix.clone(),
            confirmation_timer_queue: self.confirmation_timer_queue.clone(),
            dead_letter_exchange: self.dead_letter_exchange.clone(),
            dead_letter_queue: self.dead_letter_queue.clone(),
            conference_start_queue: self.conference_start_queue.clone(),
        }
    }
}
