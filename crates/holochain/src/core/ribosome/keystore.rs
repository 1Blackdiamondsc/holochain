use super::HostContext;
use super::WasmRibosome;
use crate::core::ribosome::RibosomeError;
use std::sync::Arc;
use sx_zome_types::KeystoreInput;
use sx_zome_types::KeystoreOutput;

pub fn keystore(
    _ribosome: Arc<WasmRibosome>,
    _host_context: Arc<HostContext>,
    _input: KeystoreInput,
) -> Result<KeystoreOutput, RibosomeError> {
    unimplemented!();
}