use futures::StreamExt;
use hdk3::prelude::*;
use holochain::conductor::{test_handle::TestConductorHandle, Conductor};
use holochain_keystore::KeystoreSender;
use holochain_state::test_utils::test_environments;
use holochain_types::{
    app::InstalledCell,
    dna::{zome::inline_zome::InlineZome, DnaFile},
};
use holochain_zome_types::element::ElementEntry;

fn simple_crud_zome() -> InlineZome {
    let entry_def = EntryDef::new(
        "entry".into(),
        Default::default(),
        Default::default(),
        Default::default(),
        Default::default(),
    );

    InlineZome::new("", vec![entry_def.clone()])
        .callback("create", move |api, ()| {
            let entry_def_id: EntryDefId = entry_def.id.clone();
            let entry = Entry::app(().try_into().unwrap()).unwrap();
            let hash = api.create((entry_def_id, entry))?;
            Ok(hash)
        })
        .callback("read", |api, hash: HeaderHash| {
            api.get((hash.into(), GetOptions::default()))
                .map_err(Into::into)
        })
}

#[tokio::test(threaded_scheduler)]
#[cfg(feature = "test_utils")]
async fn extremely_verbose_inline_zome_sketch() -> anyhow::Result<()> {
    let envs = test_environments();

    // Bundle the single zome into a DnaFile

    let (dna_file, zome) = DnaFile::unique_from_inline_zome("zome1", simple_crud_zome()).await?;
    let dna_hash = dna_file.dna_hash().clone();

    // Get two agents

    let (alice, bobbo) = TestAgent::two(envs.keystore()).await;
    let alice_cell_id = CellId::new(dna_hash.clone(), alice.clone());
    let bobbo_cell_id = CellId::new(dna_hash.clone(), bobbo.clone());

    // Create a Conductor

    let conductor: TestConductorHandle = Conductor::builder().test(&envs).await?.into();

    // Install the DNA

    conductor.install_dna(dna_file).await?;

    // Install and activate one app for Alice and another for Bob
    // TODO: develop tools to make app installation much less verbose

    conductor
        .clone()
        .install_app(
            "app:alice".to_string(),
            vec![(
                InstalledCell::new(alice_cell_id.clone(), "dna".into()),
                None,
            )],
        )
        .await?;
    conductor
        .clone()
        .install_app(
            "app:bobbo".to_string(),
            vec![(
                InstalledCell::new(bobbo_cell_id.clone(), "dna".into()),
                None,
            )],
        )
        .await?;
    conductor.activate_app("app:alice".to_string()).await?;
    conductor.activate_app("app:bobbo".to_string()).await?;
    conductor.clone().setup_cells().await?;

    // Call the "create" zome fn on Alice's app

    let hash: HeaderHash = conductor
        .call_zome(&alice_cell_id, &zome, "create", None, None, ())
        .await;

    // Wait long enough for Bob to receive gossip

    tokio::time::delay_for(std::time::Duration::from_millis(500)).await;

    // Verify that bob can run "read" on his app and get alice's Header

    let element: MaybeElement = conductor
        .call_zome(&bobbo_cell_id, &zome, "read", None, None, hash)
        .await;
    let element = element
        .0
        .expect("Element was None: bobbo couldn't `get` it");

    // Assert that the Element bob sees matches what Alice committed

    assert_eq!(*element.header().author(), alice);
    assert_eq!(
        *element.entry(),
        ElementEntry::Present(Entry::app(().try_into().unwrap()).unwrap())
    );

    Ok(())
}

// TODO: remove once host fns remove SerializedBytes constraint
#[derive(
    serde::Serialize, serde::Deserialize, Clone, Debug, SerializedBytes, shrinkwraprs::Shrinkwrap,
)]
struct MaybeElement(Option<Element>);

/// TODO: move this to a common test_utils location
pub struct TestAgent;

impl TestAgent {
    /// Get an infinite stream of AgentPubKeys
    pub fn stream(keystore: KeystoreSender) -> impl futures::Stream<Item = AgentPubKey> {
        use holochain_keystore::KeystoreSenderExt;
        futures::stream::unfold(keystore, |keystore| async {
            let key = keystore
                .generate_sign_keypair_from_pure_entropy()
                .await
                .expect("can generate AgentPubKey");
            Some((key, keystore))
        })
    }

    /// Get one AgentPubKey
    pub async fn one(keystore: KeystoreSender) -> AgentPubKey {
        let mut agents: Vec<AgentPubKey> = Self::stream(keystore).take(1).collect().await;
        agents.pop().unwrap()
    }

    /// Get two AgentPubKeys
    pub async fn two(keystore: KeystoreSender) -> (AgentPubKey, AgentPubKey) {
        let mut agents: Vec<AgentPubKey> = Self::stream(keystore).take(2).collect().await;
        (agents.pop().unwrap(), agents.pop().unwrap())
    }

    /// Get three AgentPubKeys
    pub async fn three(keystore: KeystoreSender) -> (AgentPubKey, AgentPubKey, AgentPubKey) {
        let mut agents: Vec<_> = Self::stream(keystore).take(3).collect().await;
        (
            agents.pop().unwrap(),
            agents.pop().unwrap(),
            agents.pop().unwrap(),
        )
    }
}
