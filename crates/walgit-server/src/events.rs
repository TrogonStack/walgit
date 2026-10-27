//! The webhook sink of the events bridge (`crate::bridge`). The event model
//! (`RefEvent`, `refs_from_entries`, notification parsing) lives in `walgit-events`
//! so the `JetStream` publisher shares it. Contract: `docs/EVENTS.md`.

use walgit_events::RefEvent;

/// Where the bridge publishes: a webhook receives each batch as a JSON array,
/// `POST`ed with `Content-Type: application/json`, a `X-Walgit-Delivery` id
/// (sha1 of the body — the consumer's dedup key) and, when `events.webhook_secret`
/// is set, `X-Walgit-Signature: sha256=<hex HMAC-SHA256 of the body>`. A non-2xx
/// answer fails the batch; the cursor does not move and the bridge retries.
#[async_trait::async_trait]
pub(crate) trait Sink: Send + Sync {
    fn name(&self) -> &'static str;
    async fn deliver(&self, batch: &[RefEvent]) -> anyhow::Result<()>;
}

pub(crate) struct WebhookSink {
    url: String,
    secret: Option<Vec<u8>>,
    client: reqwest::Client,
}

impl WebhookSink {
    pub fn new(url: String, secret: Option<String>) -> Self {
        WebhookSink {
            url,
            secret: secret.map(std::string::String::into_bytes),
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(10))
                .build()
                .expect("reqwest client"),
        }
    }

    /// `sha256=<hex>` over `body` with the shared secret.
    pub fn signature(secret: &[u8], body: &[u8]) -> String {
        use hmac::{Hmac, Mac};
        let mut mac = Hmac::<sha2::Sha256>::new_from_slice(secret).expect("hmac key");
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }
}

#[async_trait::async_trait]
impl Sink for WebhookSink {
    fn name(&self) -> &'static str {
        "webhook"
    }
    async fn deliver(&self, batch: &[RefEvent]) -> anyhow::Result<()> {
        use sha1::Digest;
        let body = serde_json::to_vec(batch)?;
        let delivery = hex::encode(sha1::Sha1::digest(&body));
        let mut req = self
            .client
            .post(&self.url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header("x-walgit-delivery", delivery);
        if let Some(secret) = &self.secret {
            req = req.header("x-walgit-signature", Self::signature(secret, &body));
        }
        let resp = req.body(body).send().await?;
        anyhow::ensure!(
            resp.status().is_success(),
            "webhook returned {}",
            resp.status()
        );
        Ok(())
    }
}
