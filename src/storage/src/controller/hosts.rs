// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Provisioning and assignment of storage hosts.
//!
//! A storage host is a single `storaged` process. It may host any number of
//! storage objects, where a storage object is either an ingestion (i.e., a
//! source) or a sink.
//!
//! The [`StorageHosts`] type manages provisioning of storage hosts and
//! assignment of storage objects to those hosts. The default policy is to
//! create a new storage host for each storage object, but storage objects
//! may override this policy by specifying the address of an existing storage
//! host. This policy is subject to change in the future.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet};
use std::num::NonZeroUsize;
use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_types::Codec64;
use timely::progress::Timestamp;
use tokio::sync::Mutex;
use tracing::info;

use mz_build_info::BuildInfo;
use mz_orchestrator::{NamespacedOrchestrator, ServiceConfig, ServicePort};
use mz_ore::collections::CollectionExt;
use mz_proto::RustType;
use mz_repr::GlobalId;

use crate::controller::rehydration::RehydratingStorageClient;
use crate::protocol::client::{
    ProtoStorageCommand, ProtoStorageResponse, StorageCommand, StorageResponse,
};
use crate::types::hosts::{StorageHostConfig, StorageHostResourceAllocation};

/// The network address of a storage host.
pub type StorageHostAddr = String;

/// Configuration for [`StorageHosts`].
pub struct StorageHostsConfig {
    /// The build information for this process.
    pub build_info: &'static BuildInfo,
    /// An orchestrator to start and stop storage hosts.
    pub orchestrator: Arc<dyn NamespacedOrchestrator>,
    /// The storaged image to use when starting new storage hosts.
    pub storaged_image: String,
}

/// Manages provisioning of storage hosts and assignment of storage objects
/// to those hosts.
///
/// See the [module documentation](self) for details.
#[derive(Debug)]
pub struct StorageHosts<T> {
    /// The build information for this process.
    pub build_info: &'static BuildInfo,
    /// An orchestrator to start and stop storage hosts.
    orchestrator: Arc<dyn NamespacedOrchestrator>,
    /// The storaged image to use when starting new storage hosts.
    storaged_image: String,
    /// The known storage hosts, identified by network address.
    hosts: HashMap<StorageHostAddr, StorageHost<T>>,
    /// The assignment of storage objects to storage hosts.
    objects: HashMap<GlobalId, StorageHostAddr>,
    /// Set to `true` once `initialization_complete` has been called.
    initialized: bool,
    /// A handle to Persist
    persist: Arc<Mutex<PersistClientCache>>,
}

/// Metadata about a single storage host.
#[derive(Debug)]
struct StorageHost<T> {
    /// The client to the storage host.
    client: RehydratingStorageClient<T>,
    /// The IDs of the storage objects installed on the storage host.
    objects: HashSet<GlobalId>,
    /// Whether the storage host is orchestrated.
    orchestrated: bool,
}

impl<T> StorageHosts<T>
where
    T: Timestamp + Lattice + Codec64,
    StorageCommand<T>: RustType<ProtoStorageCommand>,
    StorageResponse<T>: RustType<ProtoStorageResponse>,
{
    /// Constructs a new [`StorageHosts`] from its configuration.
    pub fn new(
        config: StorageHostsConfig,
        persist: Arc<Mutex<PersistClientCache>>,
    ) -> StorageHosts<T> {
        StorageHosts {
            build_info: config.build_info,
            orchestrator: config.orchestrator,
            storaged_image: config.storaged_image,
            objects: HashMap::new(),
            hosts: HashMap::new(),
            initialized: false,
            persist,
        }
    }

    /// Marks the end of any initialization commands.
    ///
    /// The implementor may wait for this method to be called before
    /// implementing prior commands, and so it is important for a user to invoke
    /// this method as soon as it is comfortable. This method can be invoked
    /// immediately, at the potential expense of performance.
    pub fn initialization_complete(&mut self) {
        self.initialized = true;
        for client in self.clients() {
            client.send(StorageCommand::InitializationComplete);
        }
    }

    /// Provisions a storage host for the storage object with the specified ID.
    ///
    /// If `remote_addr` is `Some`, then the specified storage host is used.
    /// Otherwise, a storage host is assigned automatically.
    ///
    /// At present, the policy for storage host assignment creates a new storage
    /// host for each storage object. This policy is subject to change.
    ///
    /// Returns a client to the provisioned host. The client may be
    /// retrieved in the future via the [`client`](StorageHosts::client)
    /// method.
    ///
    /// # Panics
    ///
    /// Panics if `id` is already provisioned.
    pub async fn provision(
        &mut self,
        id: GlobalId,
        host_config: StorageHostConfig,
    ) -> Result<&mut RehydratingStorageClient<T>, anyhow::Error>
    where
        StorageCommand<T>: RustType<ProtoStorageCommand>,
        StorageResponse<T>: RustType<ProtoStorageResponse>,
    {
        let (host_addr, orchestrated) = match host_config {
            StorageHostConfig::Remote { addr } => (addr, false),
            StorageHostConfig::Managed { allocation, .. } => {
                (self.ensure_storage_host(id, allocation).await?, true)
            }
        };

        // True iff we're reprovisioning an existing service address, instead of creating a fresh one
        let reprovisioning_addr =
            if let Some(previous_address) = self.objects.insert(id, host_addr.clone()) {
                if &host_addr == &previous_address {
                    info!("reprovisioning existing service {} for id {id}", &host_addr);
                    true
                } else {
                    info!(
                        "reprovisioning service for {id} changed address: {} -> {}",
                        &previous_address, &host_addr
                    );
                    // NB: we could clean up the service here, but better to re-ensure it below.
                    self.deprovision_host(id, previous_address).await?;
                    false
                }
            } else {
                info!("assigned storage object {id} to new service address {host_addr}");
                false
            };

        match self.hosts.entry(host_addr.clone()) {
            Entry::Vacant(entry) => {
                let mut client = RehydratingStorageClient::new(
                    host_addr,
                    self.build_info,
                    Arc::clone(&self.persist),
                );
                if self.initialized {
                    client.send(StorageCommand::InitializationComplete);
                }
                let host = entry.insert(StorageHost {
                    client,
                    objects: HashSet::from_iter([id]),
                    orchestrated,
                });
                Ok(&mut host.client)
            }
            Entry::Occupied(entry) => {
                let host = entry.into_mut();
                let inserted = host.objects.insert(id);
                assert!(
                    inserted || reprovisioning_addr,
                    "StorageHosts internally inconsistent: ID {id} partially tracked"
                );
                Ok(&mut host.client)
            }
        }
    }

    /// Deprovisions the storage host for the storage object with the specified
    /// ID.
    ///
    /// # Panics
    ///
    /// Panics if the provided `id` has not been provisioned.
    pub async fn deprovision(&mut self, id: GlobalId) -> Result<(), anyhow::Error> {
        if let Some(host_addr) = self.objects.remove(&id) {
            if self.deprovision_host(id, host_addr).await? {
                self.drop_storage_host(id).await?;
            }
        }

        Ok(())
    }

    /// Deprovision a service on a specific host. Return true if the host itself should
    /// be garbage-collected.
    pub async fn deprovision_host(
        &mut self,
        id: GlobalId,
        host_addr: StorageHostAddr,
    ) -> Result<bool, anyhow::Error> {
        match self.hosts.entry(host_addr) {
            Entry::Vacant(entry) => panic!(
                "StorageHosts internally inconsistent: \
                 ingestion {id} referenced missing storage host {}",
                entry.into_key()
            ),
            Entry::Occupied(mut entry) => {
                let host = entry.get_mut();
                let removed = host.objects.remove(&id);
                assert!(
                    removed,
                    "StorageHosts internally inconsistent: ingestion {id} backreference missing"
                );
                let empty_service = if host.objects.is_empty() {
                    let orchestrated = host.orchestrated;
                    entry.remove_entry();
                    orchestrated
                } else {
                    false
                };
                Ok(empty_service)
            }
        }
    }

    /// Retrives the client for the storage host for the given ID, if the
    /// ID is currently provisioned.
    pub fn client(&mut self, id: GlobalId) -> Option<&mut RehydratingStorageClient<T>> {
        let host_addr = self.objects.get(&id)?;
        match self.hosts.get_mut(host_addr) {
            None => panic!(
                "StorageHosts internally inconsistent: \
                 ingestion {id} referenced missing storage host {host_addr:?}"
            ),
            Some(host) => Some(&mut host.client),
        }
    }

    /// Returns an iterator over clients for all known storage hosts.
    pub fn clients(&mut self) -> impl Iterator<Item = &mut RehydratingStorageClient<T>> {
        self.hosts.values_mut().map(|h| &mut h.client)
    }

    /// Starts a orchestrated storage host for the specified ID.
    async fn ensure_storage_host(
        &self,
        id: GlobalId,
        allocation: StorageHostResourceAllocation,
    ) -> Result<StorageHostAddr, anyhow::Error> {
        let storage_service = self
            .orchestrator
            .ensure_service(
                &id.to_string(),
                ServiceConfig {
                    image: self.storaged_image.clone(),
                    args: &|assigned| {
                        vec![
                            format!("--workers=1"),
                            format!(
                                "--controller-listen-addr={}:{}",
                                assigned.listen_host, assigned.ports["controller"]
                            ),
                            format!(
                                "--internal-http-listen-addr={}:{}",
                                assigned.listen_host, assigned.ports["internal-http"]
                            ),
                            format!("--opentelemetry-resource=storage_id={}", id),
                        ]
                    },
                    ports: vec![
                        ServicePort {
                            name: "controller".into(),
                            port_hint: 2100,
                        },
                        ServicePort {
                            name: "internal-http".into(),
                            port_hint: 6878,
                        },
                    ],
                    cpu_limit: allocation.cpu_limit,
                    memory_limit: allocation.memory_limit,
                    scale: NonZeroUsize::new(1).unwrap(),
                    labels: HashMap::new(),
                    availability_zone: None,
                    // TODO: Decide on an A-A policy for storage hosts
                    anti_affinity: None,
                },
            )
            .await?;
        Ok(storage_service.addresses("controller").into_element())
    }

    /// Stops an orchestrated storage host for the specified ID.
    async fn drop_storage_host(&self, id: GlobalId) -> Result<(), anyhow::Error> {
        self.orchestrator.drop_service(&id.to_string()).await
    }
}
