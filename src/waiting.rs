//! The waiting room queue: one ordered queue per event. Handlers use the
//! `WaitingQueue` trait so the Kafka implementation can be swapped (for
//! example for SQS or `rdkafka`) without changing them.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kafkit_client::{AdminConfig, KafkaAdmin, KafkaProducer, NewTopic, ProducerConfig};

use crate::config::Config;

#[derive(Debug, thiserror::Error)]
#[error("waiting queue: {0}")]
pub struct QueueError(String);

pub type QueueResult<T> = Result<T, QueueError>;

#[async_trait]
pub trait WaitingQueue: Send + Sync {
    /// Creates the event's queue if it does not exist. Idempotent, and cheap
    /// after the first call for an event.
    async fn ensure_event(&self, event_id: i32) -> QueueResult<()>;
    /// Appends a token to the end of the event's queue and returns its offset
    /// once the broker has stored it. The offset is the token's place in line:
    /// the admission worker reads the queue in offset order.
    async fn publish(&self, event_id: i32, token_id: &str) -> QueueResult<i64>;
}

pub type SharedQueue = Arc<dyn WaitingQueue>;

pub fn topic_name(event_id: i32) -> String {
    format!("waiting.{event_id}")
}

// Queued users are admitted within hours; a few days leaves room for a
// restarted admission worker to resume from its committed offset
const RETENTION_MS: i64 = 3 * 24 * 60 * 60 * 1000;

pub struct KafkaQueue {
    producer: KafkaProducer,
    admin: KafkaAdmin,
    // Events whose topic this process has created or confirmed. Joins call
    // `ensure_event`, and a CreateTopics request per join would put a round
    // trip to the Kafka controller on the busiest endpoint.
    ensured: Mutex<HashSet<i32>>,
}

impl KafkaQueue {
    pub async fn connect(config: &Config) -> QueueResult<Self> {
        let producer = KafkaProducer::connect(
            ProducerConfig::new(config.kafka_brokers.as_str())
                .with_client_id("waiting-room-api")
                // Stored by every in-sync replica; retries cannot create duplicates
                .with_acks(-1)
                .with_enable_idempotence(true),
        )
        .await
        .map_err(|e| QueueError(format!("connect producer: {e}")))?;

        let admin = KafkaAdmin::connect(AdminConfig::new(config.kafka_brokers.as_str()).with_client_id("waiting-room-api"))
            .await
            .map_err(|e| QueueError(format!("connect admin: {e}")))?;

        log::info!("Connected to Kafka at {}", config.kafka_brokers);
        Ok(Self { producer, admin, ensured: Mutex::new(HashSet::new()) })
    }
}

#[async_trait]
impl WaitingQueue for KafkaQueue {
    async fn ensure_event(&self, event_id: i32) -> QueueResult<()> {
        if self.ensured.lock().unwrap().contains(&event_id) {
            return Ok(());
        }
        // One partition, so the order of the whole event's queue is total.
        // "Topic already exists" is treated as success by the client.
        let topic = NewTopic::new(topic_name(event_id), 1, 1).with_config("retention.ms", RETENTION_MS.to_string());
        self.admin
            .create_topics([topic])
            .await
            .map_err(|e| QueueError(format!("create topic {}: {e}", topic_name(event_id))))?;
        self.ensured.lock().unwrap().insert(event_id);
        Ok(())
    }

    async fn publish(&self, event_id: i32, token_id: &str) -> QueueResult<i64> {
        self.producer
            .send_to(topic_name(event_id), token_id.to_owned())
            .await
            .map(|ack| ack.offset)
            .map_err(|e| QueueError(format!("publish to {}: {e}", topic_name(event_id))))
    }
}
