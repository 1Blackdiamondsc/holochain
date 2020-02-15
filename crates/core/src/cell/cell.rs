use super::autonomic::AutonomicProcess;
use crate::{
    agent::SourceChain,
    cell::error::{CellError, CellResult},
    nucleus::{ZomeInvocation, ZomeInvocationResult},
    txn::{dht::DhtPersistence, source_chain, source_chain::SourceChainPersistence},
    workflow,
};
use async_trait::async_trait;
use holochain_persistence_api::txn::CursorProvider;
use std::{
    hash::{Hash, Hasher},
    path::Path,
};
use sx_types::{
    agent::AgentId,
    dna::Dna,
    error::{SkunkError, SkunkResult},
    prelude::*,
    shims::*,
};

/// TODO: consider a newtype for this
pub type DnaAddress = Address;

/// The unique identifier for a running Cell.
/// Cells are uniquely determined by this pair - this pair is necessary
/// and sufficient to refer to a cell in a conductor
pub type CellId = (DnaAddress, AgentId);


impl Hash for Cell {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        (self.dna_address(), self.agent_id()).hash(state);
    }
}

impl PartialEq for Cell {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id
    }
}

#[derive(Clone)]
pub struct Cell {
    id: CellId,
    chain_persistence: SourceChainPersistence,
    dht_persistence: DhtPersistence,
}

impl Cell {
    fn dna_address(&self) -> &DnaAddress {
        &self.id.0
    }

    fn agent_id(&self) -> &AgentId {
        &self.id.1
    }

    fn source_chain(&self) -> SourceChain {
        SourceChain::new(&self.chain_persistence)
    }

    pub async fn invoke_zome(&self, invocation: ZomeInvocation) -> CellResult<ZomeInvocationResult> {
        let source_chain = SourceChain::new(&self.chain_persistence);
        let cursor_rw = self
            .chain_persistence
            .create_cursor_rw()
            .map_err(SkunkError::from)?;
        Ok(workflow::invoke_zome(invocation, source_chain, cursor_rw).await?)
    }

    pub async fn handle_network_message(
        &self,
        msg: Lib3hToClient,
    ) -> CellResult<Option<Lib3hToClientResponse>> {
        Ok(workflow::handle_network_message(msg).await?)
    }

    pub async fn handle_autonomic_process(&self, process: AutonomicProcess) -> CellResult<()> {
        match process {
            AutonomicProcess::FastPush(entries) => workflow::publish(entries).await,
            AutonomicProcess::SlowHeal => unimplemented!(),
            AutonomicProcess::HealthCheck => unimplemented!(),
        }
    }
}

impl Cell {
    /// Checks if Cell has been initialized already
    pub fn from_id(id: CellId) -> CellResult<Self> {
        let chain_persistence = SourceChainPersistence::new(id.clone());
        let dht_persistence = DhtPersistence::new(id.clone());
        SourceChain::new(&chain_persistence).validate()?;
        Ok(Cell {
            id,
            chain_persistence,
            dht_persistence,
        })
    }

    pub fn from_dna(agent_id: AgentId, dna: Dna) -> SkunkResult<Self> {
        unimplemented!()
    }
}

pub struct CellBuilder {
    id: CellId,
    chain_persistence: Option<SourceChainPersistence>,
    dht_persistence: Option<DhtPersistence>,
}

impl CellBuilder {
    pub fn new(id: CellId) -> Self {
        Self {
            id,
            chain_persistence: None,
            dht_persistence: None,
        }
    }

    pub fn with_dna(self, dna: Dna) -> Self {
        unimplemented!()
    }

    #[cfg(test)]
    pub fn with_test_persistence(mut self, dir: &Path) -> Self {
        self.chain_persistence = Some(SourceChainPersistence::test(&dir.join("chain")));
        self.dht_persistence = Some(DhtPersistence::test(&dir.join("dht")));
        self
    }

    pub fn build(self) -> Cell {
        let id = self.id.clone();
        Cell {
            id: self.id,
            chain_persistence: self
                .chain_persistence
                .unwrap_or_else(|| SourceChainPersistence::new(id.clone())),
            dht_persistence: self
                .dht_persistence
                .unwrap_or_else(|| DhtPersistence::new(id.clone())),
        }
    }
}

// These are possibly composable traits that describe how to get a resource,
// so instead of explicitly building resources, we can downcast a Cell to exactly
// the right set of resource getter traits
trait NetSend {
    fn network_send(&self, msg: Lib3hClientProtocol) -> SkunkResult<()>;
}

/// Simplification of holochain_net::connection::NetSend
/// Could use the trait instead, but we will want an impl of it
/// for just a basic crossbeam_channel::Sender, so I'm simplifying
/// to avoid making a change to holochain_net
pub type NetSender = futures::channel::mpsc::Sender<Lib3hClientProtocol>;

#[cfg(test)]
pub mod tests {

    use super::*;
    use crate::test_utils::fake_cell_id;

    #[test]
    fn can_create_cell() {
        let tmpdir = tempdir::TempDir::new("skunkworx").unwrap();
        let cell: Cell = CellBuilder::new(fake_cell_id("a"))
            .with_test_persistence(tmpdir.path())
            .build();
    }
}