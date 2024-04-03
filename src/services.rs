use crate::controller::CLUSTER_CONFIG_FINALIZER;
use anyhow::Result;
use async_nats::{
    jetstream,
    jetstream::{
        consumer::{pull::Config, Consumer},
        stream::{Config as StreamConfig, RetentionPolicy, Source, StorageType},
        AckKind,
    },
    Client,
};
use futures::StreamExt;
use k8s_openapi::api::core::v1::{Service, ServicePort, ServiceSpec};
use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::{
    api::{Api, DeleteParams, Patch, PatchParams},
    client::Client as KubeClient,
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};
use wadm::{
    events::{Event, ManifestPublished, ManifestUnpublished},
    model::Manifest,
};

const CONSUMER_PREFIX: &str = "wasmcloud_operator_service";
const WADM_EVT_SUBJECT: &str = "wadm.evt";
const OPERATOR_STREAM_NAME: &str = "wasmcloud_operator_events";

#[derive(Clone, Debug)]
enum WatcherCommand {
    UpsertService {
        name: String,
        namespace: String,
        port: u16,
    },
    RemoveService {
        name: String,
        namespace: String,
    },
}

#[derive(Clone, Debug)]
pub struct Watcher {
    namespace: String,
    lattice_id: String,
    shutdown: CancellationToken,
    consumer: Consumer<Config>,
    tx: mpsc::Sender<WatcherCommand>,
}

impl Drop for Watcher {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

impl Watcher {
    fn new(
        namespace: String,
        lattice_id: String,
        consumer: Consumer<Config>,
        tx: mpsc::Sender<WatcherCommand>,
    ) -> Self {
        let watcher = Self {
            namespace,
            lattice_id: lattice_id.clone(),
            consumer,
            shutdown: CancellationToken::new(),
            tx,
        };

        // TODO is there a better way to handle this?
        let watcher_dup = watcher.clone();
        tokio::spawn(async move {
            tokio::select! {
                _ = watcher_dup.shutdown.cancelled() => {
                    debug!("Service watcher shutting down for lattice {}", lattice_id);
                }
                _ = watcher_dup.watch_events(&watcher_dup.consumer) => {
                    error!("Service watcher for lattice {} has stopped", lattice_id);
                }
            }
        });

        watcher
    }

    async fn watch_events(&self, consumer: &Consumer<Config>) -> Result<()> {
        let mut messages = consumer.stream().messages().await?;
        while let Some(message) = messages.next().await {
            if let Ok(message) = message {
                //message
                //    .ack_with(AckKind::Progress)
                //    .await
                //    .map_err(|e| anyhow::anyhow!(e))?;

                //let _ = self.handle_event(message.clone()).map_err(|e| {
                //    error!("Error handling event: {}", e);
                //});
                match self.handle_event(message.clone()) {
                    Ok(_) => message.ack().await.map_err(|e| anyhow::anyhow!(e))?,
                    Err(e) => {
                        error!("Error handling event: {}", e);
                        message
                            .ack_with(AckKind::Nak(None))
                            .await
                            .map_err(|e| anyhow::anyhow!(e))?;
                    }
                }
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
                let name = mp.manifest.metadata.name.clone();
                let _ = self.handle_manifest_published(mp).map_err(|e| {
                    error!(lattice_id = %self.lattice_id, manifest = name, "Error handling manifest published event: {}", e)
                });
            }
            Event::ManifestUnpublished(mu) => {
                let name = mu.name.clone();
                let _ = self.handle_manifest_unpublished(mu).map_err(|e| {
                    error!(lattice_id = %self.lattice_id, manifest = name, "Error handling manifest unpublished event: {}", e);
                });
            }
            _ => {}
        }
        Ok(())
    }

    fn handle_manifest_published(&self, mp: ManifestPublished) -> Result<()> {
        debug!("Handling manifest published event: {:?}", mp);
        let manifest = mp.manifest;
        if let Some(httpserver_service) = http_server_component(&manifest) {
            debug!("Found httpserver component: {}", httpserver_service);
            if let Some(address) = find_address(&manifest, httpserver_service.as_str()) {
                debug!("Found address: {}", address);
                if let Ok(addr) = address.parse::<SocketAddr>() {
                    debug!("Upserting service for manifest: {}", manifest.metadata.name);
                    self.tx
                        .try_send(WatcherCommand::UpsertService {
                            name: manifest.metadata.name.clone(),
                            port: addr.port(),
                            namespace: self.namespace.clone(),
                        })
                        .map_err(|e| anyhow::anyhow!("Error sending command to watcher: {}", e))?;
                } else {
                    error!("Invalid address in manifest: {}", address);
                }
            }
        }
        Ok(())
    }

    fn handle_manifest_unpublished(&self, mu: ManifestUnpublished) -> Result<()> {
        self.tx
            .try_send(WatcherCommand::RemoveService {
                name: mu.name,
                namespace: self.namespace.clone(),
            })
            .map_err(|e| anyhow::anyhow!("Error sending command to watcher: {}", e))?;
        Ok(())
    }
}

pub struct ServiceWatcher {
    watchers: Arc<RwLock<HashMap<String, Watcher>>>,
    sender: mpsc::Sender<WatcherCommand>,
}

impl ServiceWatcher {
    pub fn new(k8s_client: KubeClient) -> Self {
        // Should this be unbounded or have a larger bound?
        let (tx, mut rx) = mpsc::channel::<WatcherCommand>(1000);

        let client = k8s_client.clone();
        tokio::spawn(async move {
            while let Some(cmd) = rx.recv().await {
                match cmd {
                    WatcherCommand::UpsertService {
                        name,
                        namespace,
                        port,
                    } => {
                        create_or_update_service(
                            client.clone(),
                            &namespace,
                            name.as_str(),
                            port,
                            None,
                        )
                        .await
                        .map_err(|e| error!("Error creating/updating service: {}", e))
                        .ok();
                    }
                    WatcherCommand::RemoveService { name, namespace } => {
                        delete_service(client.clone(), &namespace, name.as_str())
                            .await
                            .map_err(|e| error!("Error deleting service: {}", e))
                            .ok();
                    }
                }
            }
        });

        Self {
            watchers: Arc::new(RwLock::new(HashMap::new())),
            sender: tx,
        }
    }

    pub async fn watch(&self, client: Client, namespace: String, lattice_id: String) -> Result<()> {
        // If we're already watching this lattice then return early
        if self.watchers.read().await.contains_key(lattice_id.as_str()) {
            return Ok(());
        }

        let js = jetstream::new(client.clone());
        let subject = format!("{WADM_EVT_SUBJECT}.{}", lattice_id.clone());

        // TODO should any of this be configurable?
        // Should we also be doing this when we first create the ServiceWatcher?
        let stream = js
            .get_or_create_stream(StreamConfig {
                name: OPERATOR_STREAM_NAME.to_string(),
                description: Some(
                    "Stream for wadm events consumed by the wasmCloud K8s Operator".to_string(),
                ),
                max_age: wadm::DEFAULT_EXPIRY_TIME,
                retention: RetentionPolicy::WorkQueue,
                storage: StorageType::File,
                allow_rollup: false,
                num_replicas: 1,
                mirror: Some(Source {
                    name: "wadm_events".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .await?;

        let consumer_name = format!("{CONSUMER_PREFIX}-{}", lattice_id.clone());
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

        let watcher = Watcher::new(namespace, lattice_id.clone(), consumer, self.sender.clone());
        self.watchers
            .write()
            .await
            .insert(lattice_id.clone(), watcher);
        Ok(())
    }

    // TODO: If you have multiple WasmCloudHostConfigs watching the same lattice then this will
    // remove the watcher for all of them. A reconcile will probably fix it, but validate that
    pub async fn stop_watch(&self, lattice_id: String) -> Result<()> {
        let mut watchers = self.watchers.write().await;
        watchers.remove(lattice_id.as_str());
        Ok(())
    }
}

pub async fn create_or_update_service(
    k8s_client: KubeClient,
    namespace: &str,
    name: &str,
    port: u16,
    owner_ref: Option<OwnerReference>,
) -> Result<()> {
    let api = Api::<Service>::namespaced(k8s_client.clone(), namespace);

    // TODO add label selector for the right wasmcloud pods in this namespace, ensure the ip
    // address is ipv4 or ipv6 based on what the address says in the manifest
    // Basically just ensure we can call the service and hit the right component
    let mut svc = Service {
        metadata: kube::api::ObjectMeta {
            name: Some(name.to_string()),
            //owner_references: Some(vec![owner_ref]),
            ..Default::default()
        },
        spec: Some(ServiceSpec {
            ports: Some(vec![ServicePort {
                name: Some("http".to_string()),
                port: port as i32,
                protocol: Some("TCP".to_string()),
                ..Default::default()
            }]),
            ..Default::default()
        }),
        ..Default::default()
    };

    if let Some(owner_ref) = owner_ref {
        svc.metadata.owner_references = Some(vec![owner_ref]);
    }

    api.patch(
        name,
        &PatchParams::apply(CLUSTER_CONFIG_FINALIZER),
        &Patch::Apply(svc),
    )
    .await?;

    Ok(())
}

pub fn http_server_component(manifest: &Manifest) -> Option<String> {
    for component in manifest.spec.components.iter() {
        if let wadm::model::Properties::Capability { properties } = &component.properties {
            debug!("Properties: {:?}", properties);
            if properties.contract == "wasmcloud:httpserver" {
                return Some(component.name.clone());
            }
        }
    }
    None
}

pub fn find_address(manifest: &Manifest, target: &str) -> Option<String> {
    for component in manifest.spec.components.iter() {
        if let wadm::model::Properties::Actor { properties: _ } = &component.properties {
            if let Some(traits) = &component.traits {
                for t in traits {
                    if let wadm::model::TraitProperty::Linkdef(props) = &t.properties {
                        if props.target == target {
                            if let Some(values) = &props.values {
                                if let Some(address) = values.get("address") {
                                    return Some(address.clone());
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    None
}

pub async fn delete_service(k8s_client: KubeClient, namespace: &str, name: &str) -> Result<()> {
    let api = Api::<Service>::namespaced(k8s_client.clone(), namespace);
    api.delete(name, &DeleteParams::default()).await?;
    Ok(())
}
