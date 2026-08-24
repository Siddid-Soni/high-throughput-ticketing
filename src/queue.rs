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

// Global configuration for confirmation timeout
const CONFIRMATION_TIMEOUT_SECONDS: i64 = 20;
const CONFIRMATION_TIMEOUT_MS: u32 = (CONFIRMATION_TIMEOUT_SECONDS * 1000) as u32;

type DbPool = Pool<ConnectionManager<PgConnection>>;
type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

// Message sent when a slot becomes available and a user can confirm their booking
#[derive(Debug, Serialize, Deserialize)]
struct SlotAvailableMessage {
    booking_id: i32,
    user_id: String,
    conference_name: String,
    confirmation_deadline: DateTime<Utc>,
}

// Message for confirmation expiration tracking
#[derive(Debug, Serialize, Deserialize)]
struct ConfirmationExpirationMessage {
    booking_id: i32,
    expiration_time: DateTime<Utc>,
    conference_name: String,
}

// Message for conference start events
#[derive(Debug, Serialize, Deserialize)]
struct ConferenceStartMessage {
    conference_name: String,
    start_time: DateTime<Utc>,
}

// Consumer for handling expired confirmation messages
struct ExpiredConfirmationConsumer {
    db_pool: DbPool,
}

impl ExpiredConfirmationConsumer {
    fn new(db_pool: DbPool) -> Self {
        Self { db_pool }
    }
}

// Consumer for handling conference start events
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
        info!("🔄 Processing expired confirmation message");
        
        let result = self.handle_expired_confirmation(channel, deliver, content).await;
        if let Err(e) = result {
            error!("❌ Failed to process expired confirmation: {:?}", e);
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
        info!("🏁 Processing conference start event");
        
        let result = self.handle_conference_start(channel, deliver, content).await;
        if let Err(e) = result {
            error!("❌ Failed to process conference start event: {:?}", e);
        }
    }
}

impl ExpiredConfirmationConsumer {
    async fn handle_expired_confirmation(&mut self, channel: &Channel, deliver: Deliver, content: Vec<u8>) -> Result<()> {
        match serde_json::from_slice::<ConfirmationExpirationMessage>(&content) {
            Ok(message) => {
                info!("⏰ Confirmation expired for booking {} from conference {}", message.booking_id, message.conference_name);
                
                match self.db_pool.get() {
                    Ok(mut conn) => {
                        // Move booking back to end of waitlist
                        match self.move_booking_to_waitlist_end(&mut conn, message.booking_id, &message.conference_name).await {
                            Ok(true) => {
                                info!("✅ Moved booking {} back to waitlist for conference {}", message.booking_id, message.conference_name);
                                
                                // 🔥 CRITICAL FIX: Automatically promote next person in line
                                if let Err(e) = self.promote_next_waitlisted_person(&mut conn, &message.conference_name, channel).await {
                                    error!("❌ Failed to promote next waitlisted person for '{}': {:?}", message.conference_name, e);
                                }
                                
                                // Acknowledge successful processing
                                if let Err(e) = channel.basic_ack(BasicAckArguments::new(deliver.delivery_tag(), false)).await {
                                    error!("Error acknowledging message: {:?}", e);
                                }
                            },
                            Ok(false) => {
                                info!("ℹ️ Booking {} was not in confirmation pending state", message.booking_id);
                                // Acknowledge - not an error condition
                                if let Err(e) = channel.basic_ack(BasicAckArguments::new(deliver.delivery_tag(), false)).await {
                                    error!("Error acknowledging message: {:?}", e);
                                }
                            },
                            Err(e) => {
                                error!("❌ Error processing expired confirmation: {:?}", e);
                                // Reject and requeue for retry
                                if let Err(e) = channel.basic_nack(BasicNackArguments::new(deliver.delivery_tag(), false, true)).await {
                                    error!("Error rejecting message: {:?}", e);
                                }
                                return Err(e);
                            }
                        }
                    },
                    Err(e) => {
                        error!("❌ Database connection error: {:?}", e);
                        // Reject and requeue
                        if let Err(e) = channel.basic_nack(BasicNackArguments::new(deliver.delivery_tag(), false, true)).await {
                            error!("Error rejecting message: {:?}", e);
                        }
                        return Err(e.into());
                    }
                }
            },
            Err(e) => {
                error!("❌ Error deserializing expired confirmation message: {:?}", e);
                // Reject without requeue - malformed message
                if let Err(e) = channel.basic_nack(BasicNackArguments::new(deliver.delivery_tag(), false, false)).await {
                    error!("Error rejecting message: {:?}", e);
                }
                return Err(e.into());
            }
        }
        
        Ok(())
    }

    // Move booking back to end of waitlist
    async fn move_booking_to_waitlist_end(&self, conn: &mut PgConnection, booking_id: i32, conference_name: &str) -> Result<bool> {
        use crate::actions::{get_conference_by_name};
        
        // Get conference ID
        let conference = get_conference_by_name(conn, conference_name)?;
        
        // Get max waitlist position for this conference
        let max_position: Option<i32> = bookings::table
            .filter(bookings::conference_id.eq(conference.conference_id))
            .filter(bookings::status.eq(BookingStatus::WAITLISTED))
            .select(diesel::dsl::max(bookings::waitlist_position))
            .first(conn)?;
            
        let new_position = max_position.unwrap_or(0) + 1;
            
        // Update booking status back to waitlisted with new position
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
    
    // Promote next waitlisted person automatically when confirmation expires
    async fn promote_next_waitlisted_person(&self, conn: &mut PgConnection, conference_name: &str, channel: &Channel) -> Result<()> {
        use crate::actions::get_conference_by_name;
        
        // Get conference
        let conference = get_conference_by_name(conn, conference_name)?;
        
        // 🔥 CRITICAL FIX: Check if slots are actually available before promoting
        if conference.available_slots <= 0 {
            info!("ℹ️ No available slots in conference '{}' - skipping auto-promotion", conference_name);
            return Ok(());
        }
        
        // Get next waitlisted booking
        let next_waitlisted = bookings::table
            .filter(bookings::conference_id.eq(conference.conference_id))
            .filter(bookings::status.eq(BookingStatus::WAITLISTED))
            .order_by(bookings::waitlist_position.asc())
            .first::<Booking>(conn)
            .optional()?;
        
        if let Some(booking) = next_waitlisted {
            // Set confirmation deadline to configurable seconds
            let deadline = Utc::now() + Duration::seconds(CONFIRMATION_TIMEOUT_SECONDS);
            
            // Update booking in database - set confirmation pending
            diesel::update(bookings::table)
                .filter(bookings::booking_id.eq(booking.booking_id))
                .set((
                    bookings::waitlist_confirmation_deadline.eq(Some(deadline.naive_utc())),
                    bookings::can_confirm.eq(true),
                    bookings::status.eq(BookingStatus::ConfirmationPending),
                    bookings::waitlist_position.eq(None::<i32>),
                ))
                .execute(conn)?;
            
            // Create confirmation expiration message and schedule it
            let expiration_msg = ConfirmationExpirationMessage {
                booking_id: booking.booking_id,
                expiration_time: deadline,
                conference_name: conference_name.to_string(),
            };
            
            // Publish message to confirmation timer queue with configurable TTL
            let serialized = serde_json::to_string(&expiration_msg)?;
            let content = serialized.as_bytes().to_vec();
            
            let properties = BasicProperties::default()
                .with_delivery_mode(2) // persistent
                .with_expiration(&CONFIRMATION_TIMEOUT_MS.to_string()) // configurable timeout in milliseconds
                .finish();
            
            let args = BasicPublishArguments::new("", "confirmation.timer");
            
            channel
                .basic_publish(
                    properties,
                    content,
                    args,
                )
                .await?;
            
            info!("🔄 Auto-promoted booking {} from waitlist for conference '{}' (slots available: {}). Confirmation expires in {} seconds at {}", 
                  booking.booking_id, conference_name, conference.available_slots, CONFIRMATION_TIMEOUT_SECONDS, deadline);
        } else {
            info!("ℹ️ No more waitlisted bookings for conference '{}' - waitlist exhausted", conference_name);
        }
        
        Ok(())
    }
}

impl ConferenceStartConsumer {
    async fn handle_conference_start(&mut self, channel: &Channel, deliver: Deliver, content: Vec<u8>) -> Result<()> {
        match serde_json::from_slice::<ConferenceStartMessage>(&content) {
            Ok(message) => {
                info!("🚀 Conference '{}' has started at {}", message.conference_name, message.start_time);
                
                match self.db_pool.get() {
                    Ok(mut conn) => {
                        // Cancel all waitlisted bookings for this conference
                        match self.process_conference_start(&mut conn, &message.conference_name, channel).await {
                            Ok(cancelled_count) => {
                                if cancelled_count > 0 {
                                    info!("✅ Cancelled {} waitlisted bookings and cleaned up queue for conference '{}'", 
                                          cancelled_count, message.conference_name);
                                }
                                
                                // Acknowledge successful processing
                                if let Err(e) = channel.basic_ack(BasicAckArguments::new(deliver.delivery_tag(), false)).await {
                                    error!("Error acknowledging message: {:?}", e);
                                }
                            },
                            Err(e) => {
                                error!("❌ Error processing conference start: {:?}", e);
                                // Reject and requeue for retry
                                if let Err(e) = channel.basic_nack(BasicNackArguments::new(deliver.delivery_tag(), false, true)).await {
                                    error!("Error rejecting message: {:?}", e);
                                }
                                return Err(e);
                            }
                        }
                    },
                    Err(e) => {
                        error!("❌ Database connection error: {:?}", e);
                        // Reject and requeue
                        if let Err(e) = channel.basic_nack(BasicNackArguments::new(deliver.delivery_tag(), false, true)).await {
                            error!("Error rejecting message: {:?}", e);
                        }
                        return Err(e.into());
                    }
                }
            },
            Err(e) => {
                error!("❌ Error deserializing conference start message: {:?}", e);
                // Reject without requeue - malformed message
                if let Err(e) = channel.basic_nack(BasicNackArguments::new(deliver.delivery_tag(), false, false)).await {
                    error!("Error rejecting message: {:?}", e);
                }
                return Err(e.into());
            }
        }
        
        Ok(())
    }

    // Process conference start - cancel waitlisted bookings and cleanup queue
    async fn process_conference_start(&self, conn: &mut PgConnection, conference_name: &str, channel: &Channel) -> Result<i32> {
        use crate::actions::get_conference_by_name;
        
        // Get conference ID
        let conference = get_conference_by_name(conn, conference_name)?;
        
        // 🔥 CRITICAL FIX: Cancel all waitlisted AND confirmation pending bookings for this conference
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
        
        // Clean up the conference-specific waitlist queue
        let queue_name = format!("{}{}.waitlist", self.waitlist_queue_prefix, conference_name);
        
        match channel.queue_delete(
            amqprs::channel::QueueDeleteArguments::new(&queue_name)
                .if_empty(false) // Delete even if not empty
                .if_unused(false) // Delete even if still has consumers
                .finish()
        ).await {
            Ok(_) => {
                info!("🗑️  Deleted queue: {}", queue_name);
            },
            Err(e) => {
                // Log but don't fail - queue might not exist or already be deleted
                warn!("⚠️  Could not delete queue {}: {:?}", queue_name, e);
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
            max_channels: 10, // Smaller pool for stability
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
        info!("Connecting to RabbitMQ with amqprs (improved)...");
        
        // Connect to RabbitMQ
        let connection = Connection::open(&OpenConnectionArguments::new(
            "localhost", 
            5672,
            "guest", 
            "guest",
        )).await?;
        
        connection
            .register_callback(DefaultConnectionCallback)
            .await?;
        
        // Create initial setup channel
        let setup_channel = connection.open_channel(None).await?;
        setup_channel
            .register_callback(DefaultChannelCallback)
            .await?;
        
        // Declare the conference events exchange (topic for routing by conference name)
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
        
        // Declare the booking events exchange (direct for specific routing)
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
            
        // Declare the dead letter exchange
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
            
        // Declare the dead letter queue
        setup_channel
            .queue_declare(
                QueueDeclareArguments::new(&self.dead_letter_queue)
                    .durable(true)
                    .finish(),
            )
            .await?;
            
        // Bind dead letter queue to dead letter exchange
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
            
        // Declare the confirmation timer queue with dead letter exchange
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
            
        // Declare the conference start queue
        setup_channel
            .queue_declare(
                QueueDeclareArguments::new(&self.conference_start_queue)
                    .durable(true)
                    .finish(),
            )
            .await?;
            
        // Bind conference start queue to conference exchange
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
        
        // Close the setup channel since we'll use pooled channels
        let _ = setup_channel.close().await;
        
        info!("Connected to RabbitMQ with amqprs and initialized queues");
        
        Ok(())
    }
    
    // Get a fresh channel with retry logic
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
    
    // Robust queue operation that handles failures gracefully with retry
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
                        // Wait before retrying with exponential backoff
                        tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;
                        delay_ms *= 2;
                    } else {
                        error!("Queue operation failed after {} attempts, giving up: {:?}", max_retries, e);
                        // Don't propagate the error - queue failures shouldn't block booking operations
                        return Ok(());
                    }
                }
            }
        }
        
        Ok(())
    }
    
    // Add a booking to the waitlist
    pub async fn add_to_waitlist(&self, booking: &Booking, conference_name: &str) -> Result<()> {
        let booking_clone = booking.clone();
        let conference_name_clone = conference_name.to_string();
        
        let operation = move || {
            let booking = booking_clone.clone();
            let conference_name = conference_name_clone.clone();
            let service = self.clone();
            
            async move {
                // Get a fresh channel for this operation
                let channel = service.get_fresh_channel().await?;
                
                // Ensure the conference waitlist queue exists
                let queue_name = format!("{}{}.waitlist", service.waitlist_queue_prefix, conference_name);
                
                // Declare the queue
                channel
                    .queue_declare(
                        QueueDeclareArguments::new(&queue_name)
                            .durable(true)
                            .finish(),
                    )
                    .await?;
                
                // Create message
                let message = SlotAvailableMessage {
                    booking_id: booking.booking_id,
                    user_id: booking.user_id.clone().unwrap_or_default(),
                    conference_name: conference_name.clone(),
                    confirmation_deadline: Utc::now(),
                };
                
                // Publish message to waitlist queue
                let serialized = serde_json::to_string(&message)?;
                let content = serialized.as_bytes().to_vec();
                
                let properties = BasicProperties::default()
                    .with_delivery_mode(2) // persistent
                    .finish();
                
                // Publish directly to the queue
                let args = BasicPublishArguments::new("", &queue_name);
                
                channel
                    .basic_publish(
                        properties,
                        content,
                        args,
                    )
                    .await?;
                
                // Close the channel after use
                let _ = channel.close().await;
                
                info!("Added booking {} to waitlist for conference {}", booking.booking_id, conference_name);
                Ok(())
            }
        };
        
        self.safe_queue_operation(operation).await
    }
    
    // Publish message when a slot becomes available
    pub async fn publish_slot_available(&self, conference_name: &str) -> Result<()> {
        let conference_name_clone = conference_name.to_string();
        
        let operation = move || {
            let conference_name = conference_name_clone.clone();
            let service = self.clone();
            
            async move {
                // Get a fresh channel for this operation
                let channel = service.get_fresh_channel().await?;
                
                // Get conference and check available slots first
                let mut conn = service.db_pool.get()?;
                
                // 🔥 CRITICAL FIX: Check if slots are actually available
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
                
                // Get next waitlisted booking from database
                let next_waitlisted = bookings::table
                    .filter(bookings::status.eq(BookingStatus::WAITLISTED))
                    .filter(bookings::conference_id.eq(conference.conference_id))
                    .order_by(bookings::waitlist_position.asc())
                    .first::<Booking>(&mut conn)
                    .optional()?;
                
                if let Some(booking) = next_waitlisted {
                    // Set confirmation deadline to configurable seconds
                    let deadline = Utc::now() + Duration::seconds(CONFIRMATION_TIMEOUT_SECONDS);
                    
                    // Update booking in database - set confirmation pending
                    diesel::update(bookings::table)
                        .filter(bookings::booking_id.eq(booking.booking_id))
                        .set((
                            bookings::waitlist_confirmation_deadline.eq(Some(deadline.naive_utc())),
                            bookings::can_confirm.eq(true),
                            bookings::status.eq(BookingStatus::ConfirmationPending),
                            bookings::waitlist_position.eq(None::<i32>),
                        ))
                        .execute(&mut conn)?;
                    
                    // Declare the confirmation timer queue with dead letter exchange
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
                    
                    // Create confirmation expiration message
                    let expiration_msg = ConfirmationExpirationMessage {
                        booking_id: booking.booking_id,
                        expiration_time: deadline,
                        conference_name: conference_name.clone(),
                    };
                    
                    // Publish message to confirmation timer queue with configurable TTL
                    let serialized = serde_json::to_string(&expiration_msg)?;
                    let content = serialized.as_bytes().to_vec();
                    
                    let properties = BasicProperties::default()
                        .with_delivery_mode(2) // persistent
                        .with_expiration(&CONFIRMATION_TIMEOUT_MS.to_string()) // configurable timeout in milliseconds
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
                        "📢 Promoted booking {} from waitlist for conference '{}' (slots available: {}). Confirmation expires in {} seconds at {}", 
                        booking.booking_id, conference_name, conference.available_slots, CONFIRMATION_TIMEOUT_SECONDS, deadline
                    );
                } else {
                    info!("No waitlisted bookings found for conference '{}'", conference_name);
                }
                
                // Close the channel after use
                let _ = channel.close().await;
                Ok(())
            }
        };
        
        self.safe_queue_operation(operation).await
    }
    
    // Start consuming messages from the dead letter queue to handle expired confirmations
    pub async fn start_consuming_expired_confirmations(&self) -> Result<()> {
        if let Some(connection) = &self.connection {
            info!("🚀 Starting dead letter queue consumer for expired confirmations");
            info!("📋 Dead letter queue name: {}", self.dead_letter_queue);
            
            let channel = connection.open_channel(None).await?;
            channel.register_callback(DefaultChannelCallback).await?;
            
            let db_pool = self.db_pool.clone();
            let dead_letter_queue = self.dead_letter_queue.clone();
            
            // Start consuming messages with manual ack
            let consumer_tag = format!("expired_confirmation_consumer_{}", Uuid::new_v4());
            let args = BasicConsumeArguments::new(&dead_letter_queue, &consumer_tag)
                .manual_ack(true)
                .finish();
            
            // Create a simple consumer to process messages
            let consumer = ExpiredConfirmationConsumer::new(db_pool);
            
            tokio::spawn(async move {
                info!("⚡ Started consuming expired confirmation messages");
                
                match channel.basic_consume(consumer, args).await {
                    Ok(_) => {
                        info!("✅ Dead letter queue consumer started successfully");
                        // Keep the consumer alive
                        loop {
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        }
                    },
                    Err(e) => {
                        error!("❌ Failed to start dead letter queue consumer: {:?}", e);
                    }
                }
            });
        } else {
            return Err("RabbitMQ connection not initialized".into());
        }
        
        Ok(())
    }

    // Start consuming conference start events - proper event-driven approach
    pub async fn start_consuming_conference_events(&self) -> Result<()> {
        if let Some(connection) = &self.connection {
            info!("🚀 Starting conference start event consumer");
            
            let channel = connection.open_channel(None).await?;
            channel.register_callback(DefaultChannelCallback).await?;
            
            let db_pool = self.db_pool.clone();
            let waitlist_queue_prefix = self.waitlist_queue_prefix.clone();
            let conference_start_queue = self.conference_start_queue.clone();
            
            // Start consuming from conference.starts queue
            tokio::spawn(async move {
                info!("⚡ Started conference start event consumer on queue: {}", conference_start_queue);
                
                let consumer_tag = format!("conference_start_consumer_{}", Uuid::new_v4());
                let args = BasicConsumeArguments::new(&conference_start_queue, &consumer_tag)
                    .manual_ack(true)
                    .finish();
                
                // Create consumer for conference start events
                let consumer = ConferenceStartConsumer::new(db_pool, waitlist_queue_prefix);
                
                match channel.basic_consume(consumer, args).await {
                    Ok(_) => {
                        info!("✅ Conference start event consumer started successfully");
                        // Keep the consumer alive
                        loop {
                            tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
                        }
                    },
                    Err(e) => {
                        error!("❌ Failed to start conference start event consumer: {:?}", e);
                    }
                }
            });
        } else {
            return Err("RabbitMQ connection not initialized".into());
        }
        
        Ok(())
    }

    // Add a booking to waitlist by booking ID
    pub async fn add_to_waitlist_by_booking_id(&self, booking_id: i32, conference_name: &str) -> Result<()> {
        let mut conn = self.db_pool.get()?;
        
        // Get the booking from database
        let booking = bookings::table
            .find(booking_id)
            .first::<Booking>(&mut conn)?;
        
        self.add_to_waitlist(&booking, conference_name).await
    }

    // Publish conference start event when a conference is created
    pub async fn schedule_conference_start_event(&self, conference_name: &str, start_time: DateTime<Utc>) -> Result<()> {
        let conference_name_clone = conference_name.to_string();
        let start_time_clone = start_time;
        
        let operation = move || {
            let conference_name = conference_name_clone.clone();
            let start_time = start_time_clone;
            let service = self.clone();
            
            async move {
                // Calculate delay until conference starts
                let now = Utc::now();
                let delay_seconds = (start_time - now).num_seconds();
                
                if delay_seconds > 0 {
                    info!("📅 Scheduled conference start event for '{}' at {} (in {} seconds)", 
                          conference_name, start_time, delay_seconds);
                    
                    // Get a fresh channel for this operation
                    let channel = service.get_fresh_channel().await?;
                    
                    // First, ensure the destination queue exists
                    channel
                        .queue_declare(
                            QueueDeclareArguments::new(&service.conference_start_queue)
                                .durable(true)
                                .finish(),
                        )
                        .await?;
                    
                    // Now set up the shared timer queue with dead letter routing
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
                    
                    // Declare the shared timer queue (idempotent - safe to call multiple times)
                    channel
                        .queue_declare(
                            QueueDeclareArguments::new(timer_queue_name)
                                .durable(true)
                                .arguments(args)
                                .finish(),
                        )
                        .await?;
                    
                    // Create the conference start message
                    let start_msg = ConferenceStartMessage {
                        conference_name: conference_name.clone(),
                        start_time,
                    };
                    
                    // Publish message to shared timer queue with TTL = delay in milliseconds
                    let serialized = serde_json::to_string(&start_msg)?;
                    let content = serialized.as_bytes().to_vec();
                    
                    let ttl_ms = (delay_seconds * 1000).max(1); // At least 1ms
                    let properties = BasicProperties::default()
                        .with_delivery_mode(2) // persistent
                        .with_expiration(&ttl_ms.to_string()) // TTL in milliseconds
                        .finish();
                    
                    let args = BasicPublishArguments::new("", timer_queue_name);
                    
                    channel.basic_publish(properties, content, args).await?;
                    
                    info!("⏰ Published conference start timer message for '{}' with TTL {}ms to shared queue", conference_name, ttl_ms);
                    info!("🔀 Message will be dead-lettered to '{}' queue when TTL expires", service.conference_start_queue);
                    
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
                    
                    info!("⚡ Conference '{}' start time has passed - triggering cleanup immediately", conference_name);
                    
                    let _ = channel.close().await;
                }
                
                Ok(())
            }
        };
        
        self.safe_queue_operation(operation).await
    }
}

// Implement Clone for WaitlistQueueService to allow cloning in the consumer tasks
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
