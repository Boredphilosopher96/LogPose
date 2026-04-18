use crate::action::{
    Action, CollectionCreateAction, ExplainArg, MetricArg, QueryAction, QueryFilter, QueryVector,
    RecordDeleteAction, RecordPutAction, WorkflowKind, parse_query_filter, parse_query_vector,
    parse_query_where,
};
use clap::{Args, Parser, Subcommand, ValueEnum};
use logpose_storage::InspectTarget;
use logpose_types::{CollectionRef, DEFAULT_DATABASE_NAME, DEFAULT_TENANT_NAME};
use std::path::PathBuf;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum OutputMode {
    Human,
    Json,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum OutputModeArg {
    Human,
    Json,
}

impl From<OutputModeArg> for OutputMode {
    fn from(value: OutputModeArg) -> Self {
        match value {
            OutputModeArg::Human => Self::Human,
            OutputModeArg::Json => Self::Json,
        }
    }
}

pub enum CommandRequest {
    Direct {
        action: Action,
        output: OutputMode,
    },
    Interactive {
        args: InteractiveArgs,
        output: OutputMode,
    },
}

#[derive(Debug, Parser)]
#[command(
    name = "logpose",
    version,
    about = "Operate LogPose clusters with a guided interactive dashboard or direct operator commands.",
    long_about = "Operate LogPose clusters with two modes: a guided interactive dashboard for discovery and a direct command surface for fast operator workflows.",
    arg_required_else_help = true
)]
pub struct Cli {
    #[arg(
        long,
        global = true,
        help = "Shortcut for --output json.",
        conflicts_with = "output"
    )]
    pub json: bool,
    #[arg(
        long,
        global = true,
        value_enum,
        value_name = "OUTPUT",
        help = "Direct command output format."
    )]
    pub output: Option<OutputModeArg>,
    #[command(subcommand)]
    pub command: Commands,
}

impl Cli {
    pub fn output_mode(&self) -> OutputMode {
        if self.json {
            OutputMode::Json
        } else {
            self.output.map(Into::into).unwrap_or(OutputMode::Human)
        }
    }

    pub fn into_request(self) -> CommandRequest {
        let output = self.output_mode();
        match self.command {
            Commands::Status => CommandRequest::Direct {
                action: Action::Status,
                output,
            },
            Commands::Config(args) => match args.command {
                ConfigCommand::Show => CommandRequest::Direct {
                    action: Action::ConfigShow,
                    output,
                },
            },
            Commands::Collection(args) => {
                let action = match args.command {
                    CollectionCommand::Create(args) => {
                        Action::CollectionCreate(CollectionCreateAction {
                            collection: args.namespace.collection_ref(args.name),
                            dimensions: args.dimensions,
                            metric: args.metric.into(),
                        })
                    }
                    CollectionCommand::Show(args) => Action::CollectionShow(args.collection_ref()),
                    CollectionCommand::Stats(args) => {
                        Action::CollectionStats(args.collection_ref())
                    }
                    CollectionCommand::Placement(args) => {
                        Action::CollectionPlacement(args.collection_ref())
                    }
                    CollectionCommand::Flush(args) => {
                        Action::CollectionFlush(args.collection_ref())
                    }
                    CollectionCommand::Compact(args) => {
                        Action::CollectionCompact(args.collection_ref())
                    }
                };
                CommandRequest::Direct { action, output }
            }
            Commands::Record(args) => {
                let action = match args.command {
                    RecordCommand::Put(args) => Action::RecordPut(RecordPutAction {
                        collection: args.collection_ref(),
                        input: args.input,
                    }),
                    RecordCommand::Delete(args) => Action::RecordDelete(RecordDeleteAction {
                        collection: args.collection_ref(),
                        id: args.id,
                    }),
                };
                CommandRequest::Direct { action, output }
            }
            Commands::Query(args) => CommandRequest::Direct {
                action: Action::Query(QueryAction {
                    collection: args.collection_ref(),
                    top_k: args.top_k,
                    vector: args.vector,
                    filters: args.filters,
                    where_clauses: args.where_clauses,
                    predicate_json: args.predicate_json,
                    explain: args.explain,
                    snapshot_manifest_generation: args.snapshot_manifest_generation,
                    snapshot_visible_seq_no: args.snapshot_visible_seq_no,
                }),
                output,
            },
            Commands::Inspect(args) => {
                let action = match args.command {
                    InspectCommand::Manifest(args) => Action::Inspect {
                        collection: args.collection_ref(),
                        target: InspectTarget::Manifest,
                    },
                    InspectCommand::Wal(args) => Action::Inspect {
                        collection: args.collection_ref(),
                        target: InspectTarget::Wal,
                    },
                    InspectCommand::Maintenance(args) => Action::Inspect {
                        collection: args.collection_ref(),
                        target: InspectTarget::Maintenance,
                    },
                    InspectCommand::Segment(args) => Action::Inspect {
                        collection: args.collection_ref(),
                        target: InspectTarget::Segment(args.segment_id),
                    },
                };
                CommandRequest::Direct { action, output }
            }
            Commands::Interactive(args) => CommandRequest::Interactive { args, output },
        }
    }
}

#[derive(Debug, Args, Clone)]
pub struct NamespaceArgs {
    #[arg(
        long,
        default_value = DEFAULT_TENANT_NAME,
        value_name = "TENANT",
        help = "Tenant containing the collection. Defaults to default."
    )]
    pub tenant: String,
    #[arg(
        long,
        default_value = DEFAULT_DATABASE_NAME,
        value_name = "DATABASE",
        help = "Database containing the collection. Defaults to default."
    )]
    pub database: String,
}

impl Default for NamespaceArgs {
    fn default() -> Self {
        Self {
            tenant: DEFAULT_TENANT_NAME.to_owned(),
            database: DEFAULT_DATABASE_NAME.to_owned(),
        }
    }
}

impl NamespaceArgs {
    pub fn collection_ref(&self, collection_name: impl Into<String>) -> CollectionRef {
        CollectionRef::new(
            self.tenant.clone(),
            self.database.clone(),
            collection_name.into(),
        )
    }
}

#[derive(Debug, Subcommand)]
pub enum Commands {
    /// Print runtime status, endpoints, and node metadata.
    Status,
    /// Inspect the effective node configuration.
    Config(ConfigGroup),
    /// Create, inspect, place, and maintain collections.
    Collection(CollectionGroup),
    /// Ingest and delete records.
    Record(RecordGroup),
    /// Run vector search with optional filters and planner diagnostics.
    Query(QueryArgs),
    /// Inspect manifest, WAL, maintenance state, or a single segment.
    Inspect(InspectGroup),
    /// Full-screen interactive dashboard with forms, result tabs, json view, and command preview.
    Interactive(InteractiveArgs),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum InteractiveWorkflowArg {
    Create,
    Show,
    Stats,
    Placement,
    Flush,
    Compact,
    Put,
    Delete,
    Query,
    InspectManifest,
    InspectWal,
    InspectMaintenance,
    InspectSegment,
    Status,
    Config,
}

impl From<InteractiveWorkflowArg> for WorkflowKind {
    fn from(value: InteractiveWorkflowArg) -> Self {
        match value {
            InteractiveWorkflowArg::Create => Self::CollectionCreate,
            InteractiveWorkflowArg::Show => Self::CollectionShow,
            InteractiveWorkflowArg::Stats => Self::CollectionStats,
            InteractiveWorkflowArg::Placement => Self::CollectionPlacement,
            InteractiveWorkflowArg::Flush => Self::CollectionFlush,
            InteractiveWorkflowArg::Compact => Self::CollectionCompact,
            InteractiveWorkflowArg::Put => Self::RecordPut,
            InteractiveWorkflowArg::Delete => Self::RecordDelete,
            InteractiveWorkflowArg::Query => Self::Query,
            InteractiveWorkflowArg::InspectManifest => Self::InspectManifest,
            InteractiveWorkflowArg::InspectWal => Self::InspectWal,
            InteractiveWorkflowArg::InspectMaintenance => Self::InspectMaintenance,
            InteractiveWorkflowArg::InspectSegment => Self::InspectSegment,
            InteractiveWorkflowArg::Status => Self::Status,
            InteractiveWorkflowArg::Config => Self::ConfigShow,
        }
    }
}

#[derive(Debug, Args, Clone)]
#[command(
    about = "Interactive dashboard with grouped workflows, guided forms, result tabs, json view, and command preview.",
    after_long_help = "Examples:\n  logpose interactive\n  logpose --json interactive --create --name colors --dimensions 768\n  logpose interactive --workflow query --collection colors --vector 1.0,0.0 --top-k 5"
)]
pub struct InteractiveArgs {
    #[command(flatten)]
    pub namespace: NamespaceArgs,
    #[arg(
        long,
        value_enum,
        value_name = "WORKFLOW",
        help = "Jump directly to a workflow. Example: create"
    )]
    pub workflow: Option<InteractiveWorkflowArg>,
    #[arg(
        long,
        conflicts_with = "workflow",
        help = "Shortcut for --workflow create"
    )]
    pub create: bool,
    #[arg(
        long,
        value_name = "COLLECTION",
        help = "Prefill collection name. Example: colors"
    )]
    pub collection: Option<String>,
    #[arg(
        long,
        value_name = "NAME",
        help = "Prefill collection name for create. Example: colors"
    )]
    pub name: Option<String>,
    #[arg(
        long,
        value_name = "DIMENSIONS",
        help = "Prefill embedding dimensions. Example: 768"
    )]
    pub dimensions: Option<usize>,
    #[arg(
        long,
        value_enum,
        value_name = "METRIC",
        help = "Prefill collection metric."
    )]
    pub metric: Option<MetricArg>,
    #[arg(
        long,
        value_name = "JSONL_PATH",
        help = "Prefill JSONL input path. Example: records.jsonl"
    )]
    pub input: Option<PathBuf>,
    #[arg(
        long,
        value_name = "RECORD_ID",
        help = "Prefill record id. Example: alpha"
    )]
    pub id: Option<String>,
    #[arg(long, value_name = "COUNT", help = "Prefill top-k. Example: 10")]
    pub top_k: Option<usize>,
    #[arg(
        long,
        value_parser = parse_query_vector,
        value_name = "VECTOR",
        help = "Prefill query vector. Example: 0.12,-0.44,0.90"
    )]
    pub vector: Option<QueryVector>,
    #[arg(
        long = "filter",
        value_parser = parse_query_filter,
        value_name = "FIELD=VALUE",
        help = "Prefill query filters. Example: kind=article"
    )]
    pub filters: Vec<QueryFilter>,
    #[arg(
        long = "where",
        value_parser = parse_query_where,
        value_name = "FIELD:OP[:VALUE]",
        help = "Prefill query predicates. Example: kind:eq:keep"
    )]
    pub where_clauses: Vec<logpose_query::Predicate>,
    #[arg(
        long,
        value_name = "PATH",
        help = "Prefill predicate JSON path. Example: predicate.json"
    )]
    pub predicate_json: Option<PathBuf>,
    #[arg(
        long,
        value_enum,
        value_name = "MODE",
        help = "Prefill query diagnostics mode."
    )]
    pub explain: Option<ExplainArg>,
    #[arg(
        long,
        value_name = "SEGMENT_ID",
        help = "Prefill segment id. Example: seg_123"
    )]
    pub segment_id: Option<String>,
}

impl InteractiveArgs {
    pub fn selected_workflow(&self) -> Option<WorkflowKind> {
        if self.create {
            Some(WorkflowKind::CollectionCreate)
        } else {
            self.workflow.map(Into::into)
        }
    }
}

#[derive(Debug, Args)]
#[command(arg_required_else_help = true)]
pub struct ConfigGroup {
    #[command(subcommand)]
    pub command: ConfigCommand,
}

#[derive(Debug, Subcommand)]
pub enum ConfigCommand {
    /// Show the effective node configuration.
    Show,
}

#[derive(Debug, Args)]
#[command(arg_required_else_help = true)]
pub struct CollectionGroup {
    #[command(subcommand)]
    pub command: CollectionCommand,
}

#[derive(Debug, Subcommand)]
pub enum CollectionCommand {
    /// Create a collection.
    Create(CollectionCreateArgs),
    /// Show metadata for a collection.
    Show(CollectionNameArg),
    /// Show collection-level storage statistics.
    Stats(CollectionNameArg),
    /// Explain where a collection is placed.
    Placement(CollectionNameArg),
    /// Flush the mutable delta into an immutable segment.
    Flush(CollectionNameArg),
    /// Compact immutable segments into a replacement segment.
    Compact(CollectionNameArg),
}

#[derive(Debug, Args)]
#[command(
    about = "Create a collection with a fixed embedding shape and distance metric.",
    after_long_help = "Examples:\n  logpose collection create colors --dimensions 768 --metric cosine\n  logpose --json collection create colors --dimensions 768 --metric cosine\n  logpose interactive"
)]
pub struct CollectionCreateArgs {
    #[command(flatten)]
    pub namespace: NamespaceArgs,
    #[arg(value_name = "NAME", help = "Collection name. Example: colors")]
    pub name: String,
    #[arg(
        long,
        value_name = "DIMENSIONS",
        help = "Embedding dimensions stored in the collection. Example: 768"
    )]
    pub dimensions: usize,
    #[arg(
        long,
        value_enum,
        value_name = "METRIC",
        help = "Distance metric used when scoring matches."
    )]
    pub metric: MetricArg,
}

#[derive(Debug, Args)]
pub struct CollectionNameArg {
    #[command(flatten)]
    pub namespace: NamespaceArgs,
    #[arg(value_name = "NAME", help = "Collection name. Example: colors")]
    pub collection: String,
}

impl CollectionNameArg {
    pub fn collection_ref(&self) -> CollectionRef {
        self.namespace.collection_ref(self.collection.clone())
    }
}

#[derive(Debug, Args)]
#[command(arg_required_else_help = true)]
pub struct RecordGroup {
    #[command(subcommand)]
    pub command: RecordCommand,
}

#[derive(Debug, Subcommand)]
pub enum RecordCommand {
    /// Ingest newline-delimited JSON records into a collection.
    Put(RecordPutArgs),
    /// Tombstone a single record id in a collection.
    Delete(RecordDeleteArgs),
}

#[derive(Debug, Args)]
#[command(
    about = "Ingest newline-delimited JSON records into a collection.",
    after_long_help = "Examples:\n  logpose record put colors --input records.jsonl\n  logpose --json record put colors --input records.jsonl\n  logpose interactive"
)]
pub struct RecordPutArgs {
    #[command(flatten)]
    pub namespace: NamespaceArgs,
    #[arg(
        value_name = "COLLECTION",
        help = "Collection to write into. Example: colors"
    )]
    pub collection: String,
    #[arg(
        long,
        value_name = "JSONL_PATH",
        help = "Path to the JSONL file containing records. Example: records.jsonl"
    )]
    pub input: PathBuf,
}

impl RecordPutArgs {
    pub fn collection_ref(&self) -> CollectionRef {
        self.namespace.collection_ref(self.collection.clone())
    }
}

#[derive(Debug, Args)]
#[command(
    about = "Tombstone a single record id in a collection.",
    after_long_help = "Examples:\n  logpose record delete colors alpha\n  logpose --json record delete colors alpha\n  logpose interactive"
)]
pub struct RecordDeleteArgs {
    #[command(flatten)]
    pub namespace: NamespaceArgs,
    #[arg(
        value_name = "COLLECTION",
        help = "Collection that contains the record. Example: colors"
    )]
    pub collection: String,
    #[arg(
        value_name = "RECORD_ID",
        help = "Record id to tombstone. Example: alpha"
    )]
    pub id: String,
}

impl RecordDeleteArgs {
    pub fn collection_ref(&self) -> CollectionRef {
        self.namespace.collection_ref(self.collection.clone())
    }
}

#[derive(Debug, Args)]
#[command(
    about = "Run vector search with optional filters, predicates, and planner diagnostics.",
    after_long_help = "Examples:\n  logpose query colors --vector 0.12,-0.44,0.90 --top-k 3\n  logpose query colors --vector 1.0,0.0 --top-k 2 --filter kind=article\n  logpose --json query colors --vector 1.0,0.0 --top-k 1 --where kind:eq:keep --explain profile\n  logpose interactive"
)]
pub struct QueryArgs {
    #[command(flatten)]
    pub namespace: NamespaceArgs,
    #[arg(
        value_name = "COLLECTION",
        help = "Collection to search. Example: colors"
    )]
    pub collection: String,
    #[arg(
        long,
        value_name = "COUNT",
        help = "Maximum number of matches to return. Example: 10"
    )]
    pub top_k: usize,
    #[arg(
        long,
        value_parser = parse_query_vector,
        value_name = "VECTOR",
        help = "Comma-separated query vector. Example: 0.12,-0.44,0.90"
    )]
    pub vector: QueryVector,
    #[arg(
        long = "filter",
        value_parser = parse_query_filter,
        value_name = "FIELD=VALUE",
        help = "Match a scalar metadata field. Examples: kind=article, score=json:7, enabled=json:true"
    )]
    pub filters: Vec<QueryFilter>,
    #[arg(
        long = "where",
        value_parser = parse_query_where,
        value_name = "FIELD:OP[:VALUE]",
        help = "Add a predicate comparison. Operators: eq, ne, lt, lte, gt, gte, exists, is_null. Example: kind:eq:keep"
    )]
    pub where_clauses: Vec<logpose_query::Predicate>,
    #[arg(
        long,
        value_name = "PATH",
        help = "Read an entire predicate JSON document from disk. Example: predicate.json"
    )]
    pub predicate_json: Option<PathBuf>,
    #[arg(
        long,
        value_enum,
        value_name = "MODE",
        help = "Return planner diagnostics. Use plan for the chosen plan or profile for timings and counters."
    )]
    pub explain: Option<ExplainArg>,
    #[arg(
        long,
        value_name = "GENERATION",
        help = "Historical manifest generation to read. Must be paired with --snapshot-visible-seq-no. Example: 12"
    )]
    pub snapshot_manifest_generation: Option<u64>,
    #[arg(
        long,
        value_name = "SEQ_NO",
        help = "Historical visible sequence number to read. Must be paired with --snapshot-manifest-generation. Example: 44"
    )]
    pub snapshot_visible_seq_no: Option<u64>,
}

impl QueryArgs {
    pub fn collection_ref(&self) -> CollectionRef {
        self.namespace.collection_ref(self.collection.clone())
    }
}

#[derive(Debug, Args)]
#[command(arg_required_else_help = true)]
pub struct InspectGroup {
    #[command(subcommand)]
    pub command: InspectCommand,
}

#[derive(Debug, Subcommand)]
pub enum InspectCommand {
    /// Inspect the active manifest.
    Manifest(CollectionNameArg),
    /// Inspect WAL records above the current checkpoint.
    Wal(CollectionNameArg),
    /// Inspect persisted maintenance state.
    Maintenance(CollectionNameArg),
    /// Inspect a single immutable segment by segment id.
    Segment(InspectSegmentArgs),
}

#[derive(Debug, Args)]
#[command(
    about = "Inspect a single immutable segment by segment id.",
    after_long_help = "Examples:\n  logpose inspect segment colors seg_123\n  logpose --json inspect segment colors seg_123\n  logpose interactive"
)]
pub struct InspectSegmentArgs {
    #[command(flatten)]
    pub namespace: NamespaceArgs,
    #[arg(
        value_name = "COLLECTION",
        help = "Collection that owns the segment. Example: colors"
    )]
    pub collection: String,
    #[arg(
        value_name = "SEGMENT_ID",
        help = "Immutable segment id. Example: seg_123"
    )]
    pub segment_id: String,
}

impl InspectSegmentArgs {
    pub fn collection_ref(&self) -> CollectionRef {
        self.namespace.collection_ref(self.collection.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn collection_commands_accept_namespace_flags() {
        let cli = Cli::try_parse_from([
            "logpose",
            "collection",
            "show",
            "colors",
            "--tenant",
            "acme",
            "--database",
            "analytics",
        ]);

        assert!(
            cli.is_ok(),
            "collection show should accept tenant/database flags: {cli:?}"
        );

        let request = cli.expect("cli should parse").into_request();
        let CommandRequest::Direct { action, .. } = request else {
            unreachable!("expected direct request");
        };
        let Action::CollectionShow(collection) = action else {
            unreachable!("expected collection show action");
        };
        assert_eq!(collection.tenant_name, "acme");
        assert_eq!(collection.database_name, "analytics");
        assert_eq!(collection.collection_name, "colors");
    }

    #[test]
    fn query_commands_accept_namespace_flags() {
        let cli = Cli::try_parse_from([
            "logpose",
            "query",
            "colors",
            "--tenant",
            "acme",
            "--database",
            "analytics",
            "--vector",
            "1.0,0.0",
            "--top-k",
            "1",
        ]);

        assert!(
            cli.is_ok(),
            "query should accept tenant/database flags: {cli:?}"
        );

        let request = cli.expect("cli should parse").into_request();
        let CommandRequest::Direct { action, .. } = request else {
            unreachable!("expected direct request");
        };
        let Action::Query(query) = action else {
            unreachable!("expected query action");
        };
        assert_eq!(query.collection.tenant_name, "acme");
        assert_eq!(query.collection.database_name, "analytics");
        assert_eq!(query.collection.collection_name, "colors");
    }

    #[test]
    fn collection_commands_default_namespace_to_default_values() {
        let cli = Cli::parse_from(["logpose", "collection", "show", "colors"]);
        let request = cli.into_request();
        let CommandRequest::Direct { action, .. } = request else {
            unreachable!("expected direct request");
        };
        let Action::CollectionShow(collection) = action else {
            unreachable!("expected collection show action");
        };
        assert_eq!(collection.tenant_name, DEFAULT_TENANT_NAME);
        assert_eq!(collection.database_name, DEFAULT_DATABASE_NAME);
        assert_eq!(collection.collection_name, "colors");
    }

    #[test]
    fn interactive_create_shortcut_preserves_prefilled_values() {
        let cli = Cli::parse_from([
            "logpose",
            "--json",
            "interactive",
            "--create",
            "--name",
            "colors",
            "--dimensions",
            "2",
        ]);

        let request = cli.into_request();
        assert!(
            matches!(request, CommandRequest::Interactive { .. }),
            "expected interactive request"
        );
        if let CommandRequest::Interactive { args, output } = request {
            assert_eq!(output, OutputMode::Json);
            assert_eq!(
                args.selected_workflow(),
                Some(WorkflowKind::CollectionCreate)
            );
            assert_eq!(args.name.as_deref(), Some("colors"));
            assert_eq!(args.dimensions, Some(2));
        }
    }
}
