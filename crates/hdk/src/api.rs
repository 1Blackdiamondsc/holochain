// #[macro_export]
// macro_rules! api_call {
//     ( $f:ident, $input:expr, $outputt:ty ) => {{
//         holochain_wasmer_guest::holochain_externs!();
//
//         let result: Result<$outputt, $crate::prelude::WasmError> = $crate::prelude::host_call!(
//             $f,
//             $input
//         );
//         result.map(|r| r.into_inner())
//     }}
// }
//
// #[macro_export]
// macro_rules! commit_entry {
//     ( $input:expr ) => {{
//         use std::convert::TryInto;
//         let try_entry: Result<Entry, SerializedBytesError> = Entry::App($input.try_into());
//         match try_entry {
//             Ok(entry) => $crate::api_call!(__commit_entry, CommitEntryInput::new(($input.into(), entry)), CommitEntryOutput),
//             Err(e) => Err(e),
//         }
//     }};
// }
//
// #[macro_export]
// macro_rules! entry_hash {
//     ( $input:expr ) => {{
//         use std::convert::TryInto;
//         let try_entry: Result<Entry, SerializedBytesError> = Entry::App($input.try_into());
//         match try_entry {
//             Ok(entry) => $crate::api_call!(__entry_hash, EntryHashInput::new(entry), EntryHashOutput),
//             Err(e) => Err(e),
//         }
//     }};
// }
//
// #[macro_export]
// macro_rules! get_entry {
//     ( $hash:expr, $options:expr ) => {{
//         $crate::api_call!(__entry_hash, GetEntryInput::new(($hash, $options)), GetEntryOutput)
//     }};
//     ( $input:expr ) => { get_entry!($input, $crate::prelude\::GetOptions) };
// }
//
// #[macro_export]
// macro_rules! link_entries {
//     ( $base:expr, $target:expr, $tag:expr ) => {{
//         $crate::api_call!(__link_entries, LinkEntriesInput::new(($base, $target, $tag.into())), LinkEntriesOutput)
//     }};
// }
//
// #[macro_export]
// macro_rules! get_links {
//     ( $base:expr, $tag:expr ) => {
//         $crate::api_call!(__get_links, GetLinksInput::new(($base, $tag.into())), GetLinksOutput)
//     }
// }