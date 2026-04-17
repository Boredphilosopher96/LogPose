#![allow(missing_docs, unused_crate_dependencies)]

use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    logpose_cli::main_entry().await
}
