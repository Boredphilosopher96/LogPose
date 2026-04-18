//! LogPose operator CLI.
#![allow(missing_docs)]

use clap as _;
use crossterm as _;
#[cfg(test)]
use insta as _;
#[cfg(test)]
use logpose_api_grpc as _;
#[cfg(test)]
use logpose_api_rest as _;
use logpose_catalog as _;
use logpose_client as _;
use logpose_config as _;
#[cfg(test)]
use logpose_core as _;
use logpose_query as _;
use logpose_storage as _;
use logpose_telemetry as _;
use logpose_types as _;
use ratatui as _;
use serde as _;
use serde_json as _;
use walkdir as _;

pub mod action;
pub mod cli;
pub mod direct;
pub mod execute;
pub mod feedback;
pub mod interactive;
pub mod render;

use anyhow::Context;
use clap::Parser;
use std::process::ExitCode;

use crate::{
    cli::{Cli, CommandRequest},
    direct::{DirectReporter, TerminalUi},
    execute::execute_action,
    interactive::run_interactive,
};

pub async fn main_entry() -> ExitCode {
    let ui = TerminalUi::detect();
    match run(&ui).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            ui.error_report(&error);
            ExitCode::FAILURE
        }
    }
}

async fn run(ui: &TerminalUi) -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = logpose_config::LogPoseConfig::load().context("failed to load configuration")?;
    logpose_telemetry::init(&config.log_filter);

    match cli.into_request() {
        CommandRequest::Direct {
            action,
            auth_token,
            output,
        } => {
            let reporter = DirectReporter::new(ui);
            let output_value =
                execute_action(&config, auth_token.as_deref(), &action, &reporter).await?;
            output_value.render_direct(output)
        }
        CommandRequest::Interactive {
            args,
            auth_token,
            output,
        } => run_interactive(&config, ui, output, auth_token, args).await,
    }
}
