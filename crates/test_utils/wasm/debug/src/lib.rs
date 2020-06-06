extern crate wee_alloc;

use holochain_wasmer_guest::*;
use holochain_zome_types::*;

// Use `wee_alloc` as the global allocator.
#[global_allocator]
static ALLOC: wee_alloc::WeeAlloc = wee_alloc::WeeAlloc::INIT;

holochain_wasmer_guest::holochain_externs!();

#[no_mangle]
pub extern "C" fn debug(_: RemotePtr) -> RemotePtr {
    let output: DebugOutput = try_result!(
        host_call!(
            __debug,
            DebugInput::new(debug_msg!("debug line numbers {}", "work"))
        ),
        "failed to call debug"
    );
    let output_sb: SerializedBytes = try_result!(
        output.try_into(),
        "failed to serialize output for extern response"
    );
    ret!(GuestOutput::new(output_sb));
}