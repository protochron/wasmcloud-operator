use anyhow::Result;
use async_nats::{
    jetstream, jetstream::consumer::pull::Config, jetstream::consumer::Consumer, Client,
};
use futures::StreamExt;
use k8s_openapi::api::core::v1::Service;
use kube::client::Client as KubeClient;
use tokio_util::sync::CancellationToken;
use tracing::error;
use wadm::events::{Event, ManifestPublished, ManifestUnpublished};

const CONSUMER_PREFIX: &str = "wasmcloud_operator_service";

pub struct ServiceWatcher {
    k8s_client: KubeClient,
    client: Client,
    lattice_id: String,
    shutdown: CancellationToken,
}

impl ServiceWatcher {
    pub fn new(
        client: Client,
        k8s_client: KubeClient,
        lattice_id: String,
        token: CancellationToken,
    ) -> Self {
        Self {
            client,
            k8s_client,
            lattice_id,
            shutdown: token,
        }
    }

    pub async fn watch_manifests(&self) -> Result<()> {
        let js = jetstream::new(self.client.clone());
        let subject = format!("wadm.evt.{}", self.lattice_id);
        let stream = js
            .get_stream(wadm::consumers::EVENTS_CONSUMER_PREFIX)
            .await
            .unwrap();
        let consumer_name = format!("{CONSUMER_PREFIX}-{}", self.lattice_id);
        let consumer = stream
        .get_or_create_consumer(
            consumer_name.as_str(),
            Config {
                durable_name: Some(consumer_name.clone()),
                description: Some("Consumer created by the wasmCloud K8s Operator to watch for new service endpoints in wadm manifests".to_string()),
                ack_policy: jetstream::consumer::AckPolicy::Explicit,
                ack_wait: std::time::Duration::from_secs(2),
                max_deliver: 3,
                deliver_policy: async_nats::jetstream::consumer::DeliverPolicy::All,
                filter_subject: subject.clone(),
                ..Default::default()
            },
        )
        .await?;
        tokio::spawn(async move {
            tokio::select! {
                _ = self.shutdown.cancelled() => {
                    //consumer.delete().await.unwrap();
                }
                _ = self.watch_events(consumer) => {}
            }
        });

        let mut messages = consumer.stream().messages().await?;
        while let Some(message) = messages.next().await {
            if let Ok(message) = message {
                let _ = self.handle_event(message).map_err(|e| {
                    error!("Error handling event: {}", e);
                });
            }
        }

        Ok(())
    }

    async fn watch_events(&self, consumer: Consumer<Config>) -> Result<()> {
        let mut messages = consumer.stream().messages().await?;
        while let Some(message) = messages.next().await {
            if let Ok(message) = message {
                let _ = self.handle_event(message).map_err(|e| {
                    error!("Error handling event: {}", e);
                });
            }
        }
        Ok(())
    }

    fn handle_event(&self, message: async_nats::jetstream::Message) -> Result<()> {
        let event = serde_json::from_slice::<cloudevents::Event>(&message.payload)
            .map_err(|e| anyhow::anyhow!("Error parsing cloudevent: {}", e))?;
        let evt = Event::try_from(event)
            .map_err(|e| anyhow::anyhow!("Error converting cloudevent to wadm event: {}", e))?;

        match evt {
            Event::ManifestPublished(mp) => {
                let _ = self.handle_manifest_published(mp).map_err(|e| {
                    error!("Error handling manifest published event: {}", e);
                });
            }
            Event::ManifestUnpublished(mu) => {
                let _ = self.handle_manifest_unpublished(mu).map_err(|e| {
                    error!("Error handling manifest unpublished event: {}", e);
                });
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_manifest_published(&self, mp: ManifestPublished) -> Result<()> {
        Ok(())
    }
    fn handle_manifest_unpublished(&self, mu: ManifestUnpublished) -> Result<()> {
        Ok(())
    }
}
