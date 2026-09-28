//! Payment providers. The pay endpoint creates an intent; the provider later
//! reports the outcome as an event in `payment_inbox`, which the payment
//! worker processes. Methods are blocking: the worker is a synchronous loop
//! (it holds an inbox row lock across Redis and Postgres calls), and the API
//! calls them inside `web::block`.

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use uuid::Uuid;

use crate::{actions, config::Config, db::DbPool, models::NewPaymentInboxEntry};

#[derive(Debug, thiserror::Error)]
#[error("payment provider: {0}")]
pub struct PaymentError(pub String);

pub type PaymentResult<T> = Result<T, PaymentError>;

pub trait PaymentProvider: Send + Sync {
    /// Name stored with inbox events
    fn name(&self) -> &'static str;
    /// Creates (or, for a repeated `idempotency_key`, returns) the intent to
    /// charge `amount_cents` for the reservation, and returns its id.
    fn create_intent(&self, reservation_id: Uuid, amount_cents: i32, idempotency_key: &str) -> PaymentResult<String>;
    /// Refunds a captured payment. Idempotent per intent.
    fn refund(&self, intent_id: &str) -> PaymentResult<()>;
}

pub type SharedProvider = Arc<dyn PaymentProvider>;

/// The provider named by `PAYMENT_PROVIDER`
pub fn from_config(config: &Config, pool: DbPool) -> SharedProvider {
    match config.payment_provider.as_str() {
        "fake" => Arc::new(FakeProvider::new(pool, Duration::from_millis(config.fake_payment_delay_ms))),
        other => panic!("PAYMENT_PROVIDER {other} is not supported"),
    }
}

/// Event type for a successful payment, the only one that confirms a booking
pub const PAYMENT_SUCCEEDED: &str = "payment_intent.succeeded";

/// What the payment worker needs from a provider event. Fake events use the
/// Stripe shape, so one parser serves both.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PaymentEvent {
    pub intent_id: String,
    pub reservation_id: Uuid,
}

/// Reads `data.object.id` and `data.object.metadata.reservation_id`.
pub fn parse_event(payload: &serde_json::Value) -> Option<PaymentEvent> {
    let object = payload.get("data")?.get("object")?;
    Some(PaymentEvent {
        intent_id: object.get("id")?.as_str()?.to_owned(),
        reservation_id: object.get("metadata")?.get("reservation_id")?.as_str()?.parse().ok()?,
    })
}

/// Succeeds every payment: `delay` after an intent is created it stores a
/// `payment_intent.succeeded` event in the inbox, as a provider webhook would.
pub struct FakeProvider {
    pool: DbPool,
    delay: Duration,
}

impl FakeProvider {
    pub fn new(pool: DbPool, delay: Duration) -> Self {
        Self { pool, delay }
    }
}

impl PaymentProvider for FakeProvider {
    fn name(&self) -> &'static str {
        "fake"
    }

    fn create_intent(&self, reservation_id: Uuid, _amount_cents: i32, idempotency_key: &str) -> PaymentResult<String> {
        // Derived from the key, so a repeated call returns the same intent
        // and its event is stored once (provider_event_id is unique)
        let intent_id = format!("fake_pi_{}", Uuid::new_v5(&Uuid::NAMESPACE_OID, idempotency_key.as_bytes()).simple());
        let payload = json!({
            "id": format!("evt_{intent_id}"),
            "type": PAYMENT_SUCCEEDED,
            "data": { "object": { "id": intent_id, "metadata": { "reservation_id": reservation_id.to_string() } } },
        });

        let (pool, delay, event_id) = (self.pool.clone(), self.delay, format!("evt_{intent_id}"));
        std::thread::spawn(move || {
            std::thread::sleep(delay);
            let entry = NewPaymentInboxEntry {
                provider: "fake",
                provider_event_id: &event_id,
                event_type: PAYMENT_SUCCEEDED,
                payload: &payload,
            };
            let stored = pool.get().map_err(|e| e.to_string()).and_then(|mut conn| {
                actions::payments::insert_inbox(&mut conn, &entry).map_err(|e| e.to_string())
            });
            if let Err(e) = stored {
                log::error!("Fake provider could not store {event_id}: {e}");
            }
        });
        Ok(intent_id)
    }

    fn refund(&self, intent_id: &str) -> PaymentResult<()> {
        log::info!("Fake provider refunded {intent_id}");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_the_stripe_event_shape() {
        let resv = Uuid::new_v4();
        let payload = json!({ "data": { "object": { "id": "pi_1", "metadata": { "reservation_id": resv.to_string() } } } });
        assert_eq!(parse_event(&payload), Some(PaymentEvent { intent_id: "pi_1".into(), reservation_id: resv }));
        assert_eq!(parse_event(&json!({ "data": { "object": { "id": "pi_1" } } })), None);
    }
}
