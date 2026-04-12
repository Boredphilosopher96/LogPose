//! LogPose operator CLI.

use anyhow::Context;
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(
    name = "logpose",
    version,
    about = "Operate LogPose clusters with fast diagnostics, administration, and data workflows."
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Cluster and node administration commands.
    Admin {
        #[command(subcommand)]
        command: AdminCommand,
    },
    /// Health, topology, and runtime diagnostics.
    Diagnostics {
        #[command(subcommand)]
        command: DiagnosticsCommand,
    },
    /// Data movement and lifecycle operations.
    Data {
        #[command(subcommand)]
        command: DataCommand,
    },
}

#[derive(Debug, Subcommand)]
enum AdminCommand {
    /// Show the effective node configuration.
    ShowConfig,
}

#[derive(Debug, Subcommand)]
enum DiagnosticsCommand {
    /// Print service endpoints and bootstrap metadata.
    Status,
}

#[derive(Debug, Subcommand)]
enum DataCommand {
    /// Describe supported ingestion workflows.
    Ingest,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    let config = logpose_config::LogPoseConfig::load().context("failed to load configuration")?;
    logpose_telemetry::init(&config.log_filter);

    match cli.command {
        Commands::Admin {
            command: AdminCommand::ShowConfig,
        } => {
            println!("{config:#?}");
        }
        Commands::Diagnostics {
            command: DiagnosticsCommand::Status,
        } => {
            println!(
                "LogPose node '{}' listening on REST {}:{} and gRPC {}:{}",
                config.node_name,
                config.rest_host,
                config.rest_port,
                config.grpc_host,
                config.grpc_port
            );
        }
        Commands::Data {
            command: DataCommand::Ingest,
        } => {
            println!(
                "LogPose data workflows support batch, streaming, and operator-driven ingestion paths."
            );
        }
    }

    Ok(())
}
