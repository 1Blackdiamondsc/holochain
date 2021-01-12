//! Helpers for unit tests

use crate::env::{DbConnection, EnvironmentKind};
use crate::prelude::BufKey;
use holochain_keystore::KeystoreSender;
use holochain_zome_types::test_utils::fake_cell_id;
use shrinkwraprs::Shrinkwrap;
use std::sync::Arc;
use tempdir::TempDir;

/// Create a [TestEnvironment] of [EnvironmentKind::Cell], backed by a temp directory.
pub fn test_cell_env() -> TestEnvironment {
    let cell_id = fake_cell_id(1);
    test_env(EnvironmentKind::Cell(cell_id))
}

/// Create a [TestEnvironment] of [EnvironmentKind::Conductor], backed by a temp directory.
pub fn test_conductor_env() -> TestEnvironment {
    test_env(EnvironmentKind::Conductor)
}

/// Create a [TestEnvironment] of [EnvironmentKind::Wasm], backed by a temp directory.
pub fn test_wasm_env() -> TestEnvironment {
    test_env(EnvironmentKind::Wasm)
}

/// Create a [TestEnvironment] of [EnvironmentKind::P2p], backed by a temp directory.
pub fn test_p2p_env() -> TestEnvironment {
    test_env(EnvironmentKind::P2p)
}

/// Generate a test keystore pre-populated with a couple test keypairs.
pub fn test_keystore() -> holochain_keystore::KeystoreSender {
    use holochain_keystore::KeystoreSenderExt;

    tokio_safe_block_on::tokio_safe_block_on(
        async move {
            let keystore = holochain_keystore::test_keystore::spawn_test_keystore()
                .await
                .unwrap();

            // pre-populate with our two fixture agent keypairs
            keystore
                .generate_sign_keypair_from_pure_entropy()
                .await
                .unwrap();
            keystore
                .generate_sign_keypair_from_pure_entropy()
                .await
                .unwrap();

            keystore
        },
        std::time::Duration::from_secs(1),
    )
    .unwrap()
}

fn test_env(kind: EnvironmentKind) -> TestEnvironment {
    let tmpdir = Arc::new(TempDir::new("holochain-test-environments").unwrap());
    TestEnvironment {
        env: DbConnection::new(tmpdir.path(), kind, test_keystore())
            .expect("Couldn't create test LMDB environment"),
        tmpdir,
    }
}

/// Create a fresh set of test environments with a new TempDir
pub fn test_environments() -> TestEnvironments {
    let tempdir = TempDir::new("holochain-test-environments").unwrap();
    TestEnvironments::new(tempdir)
}

/// A test lmdb environment with test directory
#[derive(Clone, Shrinkwrap)]
pub struct TestEnvironment {
    #[shrinkwrap(main_field)]
    /// lmdb environment
    env: DbConnection,
    /// temp directory for this environment
    tmpdir: Arc<TempDir>,
}

impl TestEnvironment {
    /// Accessor
    pub fn env(&self) -> DbConnection {
        self.env.clone()
    }

    /// Accessor
    pub fn tmpdir(&self) -> Arc<TempDir> {
        self.tmpdir.clone()
    }
}

#[derive(Clone)]
/// A container for all three non-cell environments
pub struct TestEnvironments {
    /// A test conductor environment
    conductor: DbConnection,
    /// A test wasm environment
    wasm: DbConnection,
    /// A test p2p environment
    p2p: DbConnection,
    /// The shared root temp dir for these environments
    tempdir: Arc<TempDir>,
    /// A keystore sender shared by all environments
    keystore: KeystoreSender,
}

#[allow(missing_docs)]
impl TestEnvironments {
    /// Create all three non-cell environments at once
    pub fn new(tempdir: TempDir) -> Self {
        use EnvironmentKind::*;
        let keystore = test_keystore();
        let conductor = DbConnection::new(&tempdir.path(), Conductor, keystore.clone()).unwrap();
        let wasm = DbConnection::new(&tempdir.path(), Wasm, keystore.clone()).unwrap();
        let p2p = DbConnection::new(&tempdir.path(), P2p, keystore.clone()).unwrap();
        Self {
            conductor,
            wasm,
            p2p,
            tempdir: Arc::new(tempdir),
            keystore,
        }
    }

    pub fn conductor(&self) -> DbConnection {
        self.conductor.clone()
    }

    pub fn wasm(&self) -> DbConnection {
        self.wasm.clone()
    }

    pub fn p2p(&self) -> DbConnection {
        self.p2p.clone()
    }

    /// Get the root temp dir for these environments
    pub fn tempdir(&self) -> Arc<TempDir> {
        self.tempdir.clone()
    }

    pub fn keystore(&self) -> KeystoreSender {
        self.keystore.clone()
    }
}

/// A String-based newtype suitable for database keys and values
#[derive(
    Clone,
    Debug,
    PartialOrd,
    Ord,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    derive_more::Display,
    derive_more::From,
)]
pub struct DbString(String);

impl AsRef<[u8]> for DbString {
    fn as_ref(&self) -> &[u8] {
        self.0.as_bytes()
    }
}

impl BufKey for DbString {
    fn from_key_bytes_or_friendly_panic(bytes: &[u8]) -> Self {
        Self(String::from_utf8(bytes.to_vec()).unwrap())
    }
}

impl From<&str> for DbString {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}
