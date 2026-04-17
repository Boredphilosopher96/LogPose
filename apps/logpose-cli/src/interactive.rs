use crate::{
    action::{
        Action, CollectionCreateAction, ExplainArg, MetricArg, QueryAction, RecordDeleteAction,
        RecordPutAction, WorkflowDefinition, WorkflowKind, collect_picker_files, explain_choices,
        format_command, format_filter, format_predicate, metric_choices, parse_filter_list,
        parse_query_vector, parse_where_list, picker_choice, rank_path_choices,
        rank_picker_choices, workflow_choices, workflow_definitions,
    },
    cli::{InteractiveArgs, OutputMode},
    direct::{DirectReporter, TerminalUi},
    execute::{connect_client, execute_action},
    feedback::{ProgressEvent, Reporter},
    render::{ActionOutput, command_preview},
};
use anyhow::{Context, bail};
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use logpose_config::LogPoseConfig;
use logpose_query::Predicate;
use logpose_storage::InspectTarget;
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Tabs, Wrap},
};
use std::{
    io::{self, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};
use tokio::sync::mpsc::{UnboundedSender, unbounded_channel};

pub async fn run_interactive(
    config: &LogPoseConfig,
    ui: &TerminalUi,
    output: OutputMode,
    args: InteractiveArgs,
) -> anyhow::Result<()> {
    let session = load_session_context(config).await;
    if ui.supports_fullscreen() {
        run_tui(config, output, args, session).await
    } else {
        let action = run_scripted(ui, args, &session)?;
        let reporter = DirectReporter::new(ui);
        let output_value = execute_action(config, &action, &reporter).await?;
        output_value.render_direct(output)
    }
}

#[derive(Clone, Default)]
struct SessionContext {
    collections: Vec<crate::action::PickerChoice<String>>,
    last_collection: Option<String>,
    warning: Option<String>,
}

async fn load_session_context(config: &LogPoseConfig) -> SessionContext {
    match connect_client(config).await {
        Ok(client) => match client.runtime_status().await {
            Ok(status) => SessionContext {
                collections: status
                    .collections
                    .into_iter()
                    .map(|placement| {
                        picker_choice(
                            placement.collection_name.clone(),
                            &placement.collection_name,
                            &format!(
                                "{} on {} ({})",
                                placement.route_kind,
                                placement.assigned_node,
                                placement.route_reason
                            ),
                            &[
                                &placement.route_kind,
                                &placement.assigned_node,
                                &placement.route_reason,
                            ],
                        )
                    })
                    .collect(),
                last_collection: None,
                warning: None,
            },
            Err(error) => SessionContext {
                collections: Vec::new(),
                last_collection: None,
                warning: Some(format!(
                    "Collection suggestions are unavailable until runtime status succeeds: {error}"
                )),
            },
        },
        Err(error) => SessionContext {
            collections: Vec::new(),
            last_collection: None,
            warning: Some(format!(
                "Collection suggestions are unavailable until the CLI connects to gRPC: {error}"
            )),
        },
    }
}

fn run_scripted(
    ui: &TerminalUi,
    args: InteractiveArgs,
    session: &SessionContext,
) -> anyhow::Result<Action> {
    ui.section("Interactive Mode");
    ui.info(
        "Using the scripted fallback because stdin, stdout, or stderr is not attached to a terminal.",
    );
    ui.info(
        "The full-screen dashboard is available when stdin, stdout, and stderr are all terminals.",
    );
    if let Some(warning) = &session.warning {
        ui.warn(warning);
    }

    let workflow = if let Some(workflow) = args.selected_workflow() {
        workflow
    } else {
        choose_picker_item_scripted(
            ui,
            "Choose A Workflow",
            "Workflow search",
            "query",
            &workflow_choices(),
            0,
        )?
    };

    action_from_scripted_prompts(ui, &args, workflow, session)
}

fn action_from_scripted_prompts(
    ui: &TerminalUi,
    args: &InteractiveArgs,
    workflow: WorkflowKind,
    session: &SessionContext,
) -> anyhow::Result<Action> {
    match workflow {
        WorkflowKind::CollectionCreate => {
            let name = args
                .name
                .clone()
                .filter(|value| !value.trim().is_empty())
                .map_or_else(
                    || ui.prompt_required_string("Collection name", None, Some("colors")),
                    Ok,
                )?;
            let dimensions = match args.dimensions {
                Some(dimensions) => dimensions,
                None => ui.prompt_usize("Embedding dimensions", 768)?,
            };
            let metric = match args.metric {
                Some(metric) => metric,
                None => choose_picker_item_scripted(
                    ui,
                    "Choose A Distance Metric",
                    "Distance metric search",
                    "dot",
                    &metric_choices(),
                    0,
                )?,
            };
            Ok(Action::CollectionCreate(CollectionCreateAction {
                name,
                dimensions,
                metric: metric.into(),
            }))
        }
        WorkflowKind::CollectionShow => Ok(Action::CollectionShow(required_collection_string(
            ui,
            args.collection.clone(),
            session,
            Some("colors"),
        )?)),
        WorkflowKind::CollectionStats => Ok(Action::CollectionStats(required_collection_string(
            ui,
            args.collection.clone(),
            session,
            Some("colors"),
        )?)),
        WorkflowKind::CollectionPlacement => Ok(Action::CollectionPlacement(
            required_collection_string(ui, args.collection.clone(), session, Some("colors"))?,
        )),
        WorkflowKind::CollectionFlush => {
            let collection =
                required_collection_string(ui, args.collection.clone(), session, Some("colors"))?;
            if !ui.confirm(&format!("Flush collection '{collection}' now?"), false)? {
                bail!("operation cancelled");
            }
            Ok(Action::CollectionFlush(collection))
        }
        WorkflowKind::CollectionCompact => {
            let collection =
                required_collection_string(ui, args.collection.clone(), session, Some("colors"))?;
            if !ui.confirm(&format!("Compact collection '{collection}' now?"), false)? {
                bail!("operation cancelled");
            }
            Ok(Action::CollectionCompact(collection))
        }
        WorkflowKind::RecordPut => {
            let collection =
                required_collection_string(ui, args.collection.clone(), session, Some("colors"))?;
            let input = if let Some(path) = args.input.clone() {
                path
            } else {
                choose_file_path_scripted(ui)?
            };
            Ok(Action::RecordPut(RecordPutAction { collection, input }))
        }
        WorkflowKind::RecordDelete => {
            let collection =
                required_collection_string(ui, args.collection.clone(), session, Some("colors"))?;
            let id = required_string(ui, "Record id", args.id.clone(), Some("alpha"))?;
            if !ui.confirm(
                &format!("Delete record '{id}' from collection '{collection}' now?"),
                false,
            )? {
                bail!("operation cancelled");
            }
            Ok(Action::RecordDelete(RecordDeleteAction { collection, id }))
        }
        WorkflowKind::Query => {
            let collection =
                required_collection_string(ui, args.collection.clone(), session, Some("colors"))?;
            let top_k = match args.top_k {
                Some(top_k) => top_k,
                None => ui.prompt_usize("Top K", 10)?,
            };
            let vector = if let Some(vector) = args.vector.clone() {
                vector
            } else {
                ui.prompt_required_parsed(
                    "Query vector",
                    None,
                    Some("0.12,-0.44,0.90"),
                    parse_query_vector,
                )?
            };
            let filters = if args.filters.is_empty() {
                ui.prompt_optional_parsed(
                    "Metadata filters (optional)",
                    Some("kind=article;enabled=json:true"),
                    parse_filter_list,
                )?
                .unwrap_or_default()
            } else {
                args.filters.clone()
            };
            let where_clauses = if args.where_clauses.is_empty() {
                ui.prompt_optional_parsed(
                    "Where clauses (optional)",
                    Some("kind:eq:keep"),
                    parse_where_list,
                )?
                .unwrap_or_default()
            } else {
                args.where_clauses.clone()
            };
            let explain = if let Some(explain) = args.explain {
                Some(explain)
            } else {
                choose_picker_item_scripted(
                    ui,
                    "Choose Query Diagnostics",
                    "Diagnostics search",
                    "none",
                    &explain_choices(),
                    0,
                )?
            };
            let predicate_json = args.predicate_json.clone().or(ui
                .prompt_optional_string("Predicate JSON path (optional)", Some("predicate.json"))?
                .map(PathBuf::from));
            Ok(Action::Query(QueryAction {
                collection,
                top_k,
                vector,
                filters,
                where_clauses,
                predicate_json,
                explain,
                snapshot_manifest_generation: None,
                snapshot_visible_seq_no: None,
            }))
        }
        WorkflowKind::InspectManifest => Ok(Action::Inspect {
            collection: required_collection_string(
                ui,
                args.collection.clone(),
                session,
                Some("colors"),
            )?,
            target: InspectTarget::Manifest,
        }),
        WorkflowKind::InspectWal => Ok(Action::Inspect {
            collection: required_collection_string(
                ui,
                args.collection.clone(),
                session,
                Some("colors"),
            )?,
            target: InspectTarget::Wal,
        }),
        WorkflowKind::InspectMaintenance => Ok(Action::Inspect {
            collection: required_collection_string(
                ui,
                args.collection.clone(),
                session,
                Some("colors"),
            )?,
            target: InspectTarget::Maintenance,
        }),
        WorkflowKind::InspectSegment => Ok(Action::Inspect {
            collection: required_collection_string(
                ui,
                args.collection.clone(),
                session,
                Some("colors"),
            )?,
            target: InspectTarget::Segment(required_string(
                ui,
                "Segment id",
                args.segment_id.clone(),
                Some("seg_123"),
            )?),
        }),
        WorkflowKind::Status => Ok(Action::Status),
        WorkflowKind::ConfigShow => Ok(Action::ConfigShow),
    }
}

fn required_string(
    ui: &TerminalUi,
    label: &str,
    preset: Option<String>,
    example: Option<&str>,
) -> anyhow::Result<String> {
    if let Some(value) = preset.filter(|value| !value.trim().is_empty()) {
        Ok(value)
    } else {
        ui.prompt_required_string(label, None, example)
    }
}

fn required_collection_string(
    ui: &TerminalUi,
    preset: Option<String>,
    session: &SessionContext,
    example: Option<&str>,
) -> anyhow::Result<String> {
    if let Some(value) = preset.filter(|value| !value.trim().is_empty()) {
        return Ok(value);
    }
    if session.collections.is_empty() {
        return ui.prompt_required_string("Collection name", None, example);
    }

    let default_search = session
        .last_collection
        .as_deref()
        .or_else(|| {
            session
                .collections
                .first()
                .map(|choice| choice.label.as_str())
        })
        .unwrap_or("collection");
    let default_index = session
        .last_collection
        .as_ref()
        .and_then(|value| {
            session
                .collections
                .iter()
                .position(|choice| choice.value == *value)
        })
        .unwrap_or(0);

    choose_picker_item_scripted(
        ui,
        "Choose A Collection",
        "Collection search",
        default_search,
        &session.collections,
        default_index,
    )
}

fn choose_picker_item_scripted<T: Clone>(
    ui: &TerminalUi,
    title: &str,
    search_prompt: &str,
    default_search: &str,
    choices: &[crate::action::PickerChoice<T>],
    default_index: usize,
) -> anyhow::Result<T> {
    ui.section(title);
    loop {
        let query = ui.prompt_required_string(search_prompt, Some(default_search), None)?;
        let ranked = rank_picker_choices(choices, &query, default_index);
        if ranked.is_empty() {
            ui.warn("No matches found. Try another search.");
            continue;
        }
        for (index, choice) in ranked.iter().take(8).enumerate() {
            ui.print_choice(index + 1, &choice.label, &choice.detail);
        }
        let selection = ui.prompt_usize("Select option", 1)?;
        let Some(choice) = ranked.get(selection.saturating_sub(1)) else {
            ui.warn("Select one of the listed options.");
            continue;
        };
        return Ok(choice.value.clone());
    }
}

fn choose_file_path_scripted(ui: &TerminalUi) -> anyhow::Result<PathBuf> {
    ui.section("Choose A File");
    let cwd = std::env::current_dir().context("failed to determine current working directory")?;
    let files = collect_picker_files(&cwd)?;

    if files.is_empty() {
        ui.warn(
            "No files were found under the current directory. Falling back to manual path entry.",
        );
        return Ok(PathBuf::from(ui.prompt_required_string(
            "JSONL input path",
            None,
            Some("records.jsonl"),
        )?));
    }

    let default_search = files
        .iter()
        .find(|path| path.extension().and_then(|extension| extension.to_str()) == Some("jsonl"))
        .and_then(|path| path.file_name())
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| "jsonl".to_owned());

    loop {
        let query = ui.prompt_required_string(
            "File search",
            Some(&default_search),
            Some("type part of the file name"),
        )?;
        let direct_path = PathBuf::from(&query);
        if direct_path.is_file() {
            return Ok(direct_path);
        }

        let ranked = rank_path_choices(&files, &cwd, &query);
        if ranked.is_empty() {
            ui.warn("No files matched that search.");
            if let Some(manual_path) =
                ui.prompt_optional_string("Manual path (optional)", Some("records.jsonl"))?
            {
                return Ok(PathBuf::from(manual_path));
            }
            continue;
        }

        for (index, choice) in ranked.iter().take(8).enumerate() {
            ui.print_choice(index + 1, &choice.display, "file picker result");
        }
        let selection = ui.prompt_usize("Select option", 1)?;
        let Some(choice) = ranked.get(selection.saturating_sub(1)) else {
            ui.warn("Select one of the listed file picker results.");
            continue;
        };
        return Ok(choice.path.clone());
    }
}

async fn run_tui(
    config: &LogPoseConfig,
    output_mode: OutputMode,
    args: InteractiveArgs,
    session: SessionContext,
) -> anyhow::Result<()> {
    let (mut terminal, _guard) = setup_terminal()?;
    let cwd = std::env::current_dir().context("failed to determine current working directory")?;
    let (tx, mut rx) = unbounded_channel();
    let mut app = InteractiveApp::new(args, cwd, output_mode, session)?;
    let tick_rate = Duration::from_millis(80);
    let mut last_tick = Instant::now();

    loop {
        terminal.draw(|frame| app.render(frame))?;

        while let Ok(event) = rx.try_recv() {
            app.apply_tui_event(event);
        }
        if app.should_exit {
            break;
        }

        let timeout = tick_rate.saturating_sub(last_tick.elapsed());
        if event::poll(timeout)?
            && let Event::Key(key) = event::read()?
            && app.handle_key(key, config, tx.clone()).await?
        {
            break;
        }
        if last_tick.elapsed() >= tick_rate {
            app.on_tick();
            last_tick = Instant::now();
        }
    }

    Ok(())
}

fn setup_terminal() -> anyhow::Result<(Terminal<CrosstermBackend<io::Stdout>>, TerminalGuard)> {
    enable_raw_mode().context("failed to enable raw mode")?;
    // Install the cleanup guard immediately after enabling raw mode so any
    // subsequent initialization failure restores the terminal on unwind.
    let guard = TerminalGuard;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen).context("failed to enter alternate screen")?;
    let backend = CrosstermBackend::new(stdout);
    let terminal = Terminal::new(backend).context("failed to initialize terminal")?;
    Ok((terminal, guard))
}

struct TerminalGuard;

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let _ = execute!(io::stdout(), LeaveAlternateScreen);
        let _ = disable_raw_mode();
    }
}

#[derive(Clone)]
struct ChannelReporter {
    tx: UnboundedSender<TuiEvent>,
}

impl Reporter for ChannelReporter {
    fn emit(&self, event: ProgressEvent) {
        let _ = self.tx.send(TuiEvent::Progress(event));
    }
}

enum TuiEvent {
    Progress(ProgressEvent),
    ActionComplete {
        action: Action,
        result: Box<anyhow::Result<ActionOutput>>,
        session: Box<SessionContext>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Screen {
    Home,
    Dashboard,
    Form,
    Confirm,
    Running,
    Result,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResultTab {
    Summary,
    Json,
    Command,
}

#[derive(Clone)]
struct DashboardState {
    concern: ConcernKind,
    search: String,
    selected: usize,
}

#[derive(Clone)]
struct HomeState {
    search: String,
    selected: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ConcernKind {
    Data,
    Collections,
    Runtime,
    Storage,
}

struct ConcernDefinition {
    kind: ConcernKind,
    title: &'static str,
    detail: &'static str,
    aliases: &'static [&'static str],
    workflows: &'static [WorkflowKind],
}

struct ConfirmState {
    action: Action,
    prompt: String,
    selected_yes: bool,
}

struct RunningState {
    action: Action,
    message: String,
    frame: usize,
}

struct ResultState {
    action: Action,
    output: ActionOutput,
    tab: ResultTab,
}

#[derive(Clone)]
struct PickerState {
    title: String,
    field_key: &'static str,
    query: String,
    default_index: usize,
    choices: Vec<crate::action::PickerChoice<String>>,
    selected: usize,
    empty_message: &'static str,
}

impl PickerState {
    fn filtered_choices(&self) -> Vec<&crate::action::PickerChoice<String>> {
        rank_picker_choices(&self.choices, &self.query, self.default_index)
    }
}

#[derive(Clone)]
enum FieldKind {
    Text,
    Number,
    Path,
    Collection,
    Choice(Vec<&'static str>),
}

#[derive(Clone)]
struct FormField {
    key: &'static str,
    label: &'static str,
    help: &'static str,
    placeholder: &'static str,
    required: bool,
    kind: FieldKind,
    value: String,
}

struct FormState {
    workflow: WorkflowKind,
    title: String,
    description: String,
    fields: Vec<FormField>,
    selected: usize,
    error: Option<String>,
    file_suggestions: Vec<PathBuf>,
}

impl FormState {
    fn from_workflow(
        workflow: WorkflowKind,
        args: &InteractiveArgs,
        cwd: &Path,
        session: &SessionContext,
    ) -> anyhow::Result<Self> {
        let title = workflow_title(workflow).to_owned();
        let description = workflow_description(workflow).to_owned();
        let file_suggestions = collect_picker_files(cwd).unwrap_or_default();
        let fields = match workflow {
            WorkflowKind::CollectionCreate => vec![
                text_field(
                    "name",
                    "Collection name",
                    "Stable collection name.",
                    "colors",
                    true,
                    args.name.clone(),
                ),
                number_field(
                    "dimensions",
                    "Embedding dimensions",
                    "Vector width stored in the collection.",
                    "768",
                    true,
                    args.dimensions.map(|value| value.to_string()),
                ),
                choice_field(
                    "metric",
                    "Distance metric",
                    "Similarity metric used during ranking.",
                    vec!["dot", "cosine", "l2"],
                    args.metric.map(metric_to_value),
                    "dot",
                ),
            ],
            WorkflowKind::CollectionShow
            | WorkflowKind::CollectionStats
            | WorkflowKind::CollectionPlacement
            | WorkflowKind::CollectionFlush
            | WorkflowKind::CollectionCompact
            | WorkflowKind::InspectManifest
            | WorkflowKind::InspectWal
            | WorkflowKind::InspectMaintenance => vec![collection_field(
                "collection",
                "Collection name",
                "Collection to inspect or operate on.",
                "colors",
                true,
                args.collection.clone(),
                session,
            )],
            WorkflowKind::RecordPut => vec![
                collection_field(
                    "collection",
                    "Collection name",
                    "Collection to write into.",
                    "colors",
                    true,
                    args.collection.clone(),
                    session,
                ),
                path_field(
                    "input",
                    "JSONL input path",
                    "Relative or absolute path to newline-delimited records.",
                    "records.jsonl",
                    true,
                    args.input
                        .as_ref()
                        .map(|path| path.to_string_lossy().into_owned()),
                ),
            ],
            WorkflowKind::RecordDelete => vec![
                collection_field(
                    "collection",
                    "Collection name",
                    "Collection that owns the record.",
                    "colors",
                    true,
                    args.collection.clone(),
                    session,
                ),
                text_field(
                    "id",
                    "Record id",
                    "Record identifier to delete.",
                    "alpha",
                    true,
                    args.id.clone(),
                ),
            ],
            WorkflowKind::Query => vec![
                collection_field(
                    "collection",
                    "Collection name",
                    "Collection to search.",
                    "colors",
                    true,
                    args.collection.clone(),
                    session,
                ),
                number_field(
                    "top_k",
                    "Top K",
                    "Maximum number of matches.",
                    "10",
                    true,
                    args.top_k.map(|value| value.to_string()),
                ),
                text_field(
                    "vector",
                    "Query vector",
                    "Comma-separated vector components.",
                    "0.12,-0.44,0.90",
                    true,
                    args.vector.as_ref().map(vector_to_value),
                ),
                text_field(
                    "filters",
                    "Metadata filters",
                    "Optional filters separated by ';'.",
                    "kind=article;enabled=json:true",
                    false,
                    if args.filters.is_empty() {
                        None
                    } else {
                        Some(
                            args.filters
                                .iter()
                                .map(filter_to_value)
                                .collect::<Vec<_>>()
                                .join(";"),
                        )
                    },
                ),
                text_field(
                    "where",
                    "Where clauses",
                    "Optional predicates separated by ';'.",
                    "kind:eq:keep",
                    false,
                    if args.where_clauses.is_empty() {
                        None
                    } else {
                        Some(
                            args.where_clauses
                                .iter()
                                .map(predicate_to_value)
                                .collect::<Vec<_>>()
                                .join(";"),
                        )
                    },
                ),
                choice_field(
                    "explain",
                    "Diagnostics",
                    "Optional query diagnostics mode.",
                    vec!["none", "plan", "profile"],
                    args.explain.map(explain_to_value),
                    "none",
                ),
                path_field(
                    "predicate_json",
                    "Predicate JSON path",
                    "Optional predicate document path.",
                    "predicate.json",
                    false,
                    args.predicate_json
                        .as_ref()
                        .map(|path| path.to_string_lossy().into_owned()),
                ),
            ],
            WorkflowKind::InspectSegment => vec![
                collection_field(
                    "collection",
                    "Collection name",
                    "Collection that owns the segment.",
                    "colors",
                    true,
                    args.collection.clone(),
                    session,
                ),
                text_field(
                    "segment_id",
                    "Segment id",
                    "Immutable segment identifier.",
                    "seg_123",
                    true,
                    args.segment_id.clone(),
                ),
            ],
            WorkflowKind::Status | WorkflowKind::ConfigShow => Vec::new(),
        };
        Ok(Self {
            workflow,
            title,
            description,
            fields,
            selected: 0,
            error: None,
            file_suggestions,
        })
    }

    fn current_field_mut(&mut self) -> Option<&mut FormField> {
        self.fields.get_mut(self.selected)
    }

    fn current_field(&self) -> Option<&FormField> {
        self.fields.get(self.selected)
    }

    fn field_mut(&mut self, key: &str) -> Option<&mut FormField> {
        self.fields.iter_mut().find(|field| field.key == key)
    }

    fn field_index(&self, key: &str) -> Option<usize> {
        self.fields.iter().position(|field| field.key == key)
    }

    fn move_next(&mut self) {
        if !self.fields.is_empty() {
            self.selected = (self.selected + 1) % self.fields.len();
        }
    }

    fn move_prev(&mut self) {
        if !self.fields.is_empty() {
            self.selected = if self.selected == 0 {
                self.fields.len() - 1
            } else {
                self.selected - 1
            };
        }
    }

    fn apply_key(&mut self, key: KeyEvent) {
        match key.code {
            KeyCode::Tab | KeyCode::Down => self.move_next(),
            KeyCode::BackTab | KeyCode::Up => self.move_prev(),
            KeyCode::Left => self.adjust_choice(-1),
            KeyCode::Right => self.adjust_choice(1),
            KeyCode::Backspace => {
                if let Some(field) = self.current_field_mut() {
                    match field.kind {
                        FieldKind::Text
                        | FieldKind::Number
                        | FieldKind::Path
                        | FieldKind::Collection => {
                            field.value.pop();
                        }
                        FieldKind::Choice(_) => {}
                    }
                }
            }
            KeyCode::Char(character) => {
                if key.modifiers.contains(KeyModifiers::CONTROL) {
                    return;
                }
                if let Some(field) = self.current_field_mut() {
                    match field.kind {
                        FieldKind::Text | FieldKind::Path | FieldKind::Collection => {
                            field.value.push(character)
                        }
                        FieldKind::Number => {
                            if character.is_ascii_digit() {
                                field.value.push(character);
                            }
                        }
                        FieldKind::Choice(_) => {}
                    }
                }
            }
            _ => {}
        }
    }

    fn adjust_choice(&mut self, delta: isize) {
        let Some(field) = self.current_field_mut() else {
            return;
        };
        let FieldKind::Choice(options) = &field.kind else {
            return;
        };
        let current_index = options
            .iter()
            .position(|option| *option == field.value)
            .unwrap_or(0);
        let next_index = if delta.is_negative() {
            current_index.saturating_sub(delta.unsigned_abs())
        } else {
            current_index + delta as usize
        } % options.len();
        field.value = options[next_index].to_owned();
    }

    fn build_action(&self, cwd: &Path) -> anyhow::Result<Action> {
        let field = |key: &str| -> anyhow::Result<&str> {
            self.fields
                .iter()
                .find(|field| field.key == key)
                .map(|field| field.value.trim())
                .ok_or_else(|| anyhow::anyhow!("missing form field '{key}'"))
        };
        let required = |key: &str| -> anyhow::Result<String> {
            let value = field(key)?.trim();
            if value.is_empty() {
                bail!("{key} is required");
            }
            Ok(value.to_owned())
        };
        let optional_path = |key: &str| -> anyhow::Result<Option<PathBuf>> {
            let value = field(key)?.trim();
            if value.is_empty() {
                return Ok(None);
            }
            Ok(Some(resolve_user_path(cwd, value)))
        };

        match self.workflow {
            WorkflowKind::CollectionCreate => {
                Ok(Action::CollectionCreate(CollectionCreateAction {
                    name: required("name")?,
                    dimensions: required("dimensions")?
                        .parse::<usize>()
                        .context("dimensions must be a positive integer")?,
                    metric: parse_metric(field("metric")?)?.into(),
                }))
            }
            WorkflowKind::CollectionShow => Ok(Action::CollectionShow(required("collection")?)),
            WorkflowKind::CollectionStats => Ok(Action::CollectionStats(required("collection")?)),
            WorkflowKind::CollectionPlacement => {
                Ok(Action::CollectionPlacement(required("collection")?))
            }
            WorkflowKind::CollectionFlush => Ok(Action::CollectionFlush(required("collection")?)),
            WorkflowKind::CollectionCompact => {
                Ok(Action::CollectionCompact(required("collection")?))
            }
            WorkflowKind::RecordPut => Ok(Action::RecordPut(RecordPutAction {
                collection: required("collection")?,
                input: optional_path("input")?
                    .ok_or_else(|| anyhow::anyhow!("input is required"))?,
            })),
            WorkflowKind::RecordDelete => Ok(Action::RecordDelete(RecordDeleteAction {
                collection: required("collection")?,
                id: required("id")?,
            })),
            WorkflowKind::Query => Ok(Action::Query(QueryAction {
                collection: required("collection")?,
                top_k: required("top_k")?
                    .parse::<usize>()
                    .context("top_k must be a positive integer")?,
                vector: parse_query_vector(&required("vector")?).map_err(anyhow::Error::msg)?,
                filters: parse_optional_filters(field("filters")?)?,
                where_clauses: parse_optional_predicates(field("where")?)?,
                explain: parse_explain(field("explain")?)?,
                predicate_json: optional_path("predicate_json")?,
                snapshot_manifest_generation: None,
                snapshot_visible_seq_no: None,
            })),
            WorkflowKind::InspectManifest => Ok(Action::Inspect {
                collection: required("collection")?,
                target: InspectTarget::Manifest,
            }),
            WorkflowKind::InspectWal => Ok(Action::Inspect {
                collection: required("collection")?,
                target: InspectTarget::Wal,
            }),
            WorkflowKind::InspectMaintenance => Ok(Action::Inspect {
                collection: required("collection")?,
                target: InspectTarget::Maintenance,
            }),
            WorkflowKind::InspectSegment => Ok(Action::Inspect {
                collection: required("collection")?,
                target: InspectTarget::Segment(required("segment_id")?),
            }),
            WorkflowKind::Status => Ok(Action::Status),
            WorkflowKind::ConfigShow => Ok(Action::ConfigShow),
        }
    }
}

struct InteractiveApp {
    screen: Screen,
    home: HomeState,
    dashboard: DashboardState,
    concerns: Vec<ConcernDefinition>,
    definitions: Vec<WorkflowDefinition>,
    form: Option<FormState>,
    confirm: Option<ConfirmState>,
    picker: Option<PickerState>,
    running: Option<RunningState>,
    result: Option<ResultState>,
    session: SessionContext,
    output_mode: OutputMode,
    cwd: PathBuf,
    status_message: String,
    should_exit: bool,
    // Set when the user asks to quit while an action is still in flight.
    // The TUI event loop honors this after the in-flight action completes so
    // that background mutations (e.g. batched record writes) are not killed
    // mid-flight, which would otherwise leave silent partial writes.
    exit_after_running: bool,
}

impl InteractiveApp {
    fn new(
        args: InteractiveArgs,
        cwd: PathBuf,
        output_mode: OutputMode,
        session: SessionContext,
    ) -> anyhow::Result<Self> {
        let mut app = Self {
            screen: Screen::Home,
            home: HomeState {
                search: String::new(),
                selected: 0,
            },
            dashboard: DashboardState {
                concern: ConcernKind::Data,
                search: String::new(),
                selected: 0,
            },
            concerns: concern_definitions(),
            definitions: workflow_definitions(),
            form: None,
            confirm: None,
            picker: None,
            running: None,
            result: None,
            status_message: session.warning.clone().unwrap_or_else(|| {
                "Interactive workspace ready. Pick an area to begin.".to_owned()
            }),
            session,
            output_mode,
            cwd,
            should_exit: false,
            exit_after_running: false,
        };
        if let Some(workflow) = args.selected_workflow() {
            app.dashboard.concern = concern_for_workflow(workflow);
            app.open_form(workflow, &args)?;
        }
        Ok(app)
    }

    async fn handle_key(
        &mut self,
        key: KeyEvent,
        config: &LogPoseConfig,
        tx: UnboundedSender<TuiEvent>,
    ) -> anyhow::Result<bool> {
        if self.picker.is_some() {
            return Ok(self.handle_picker_key(key));
        }
        match self.screen {
            Screen::Home => Ok(self.handle_home_key(key)),
            Screen::Dashboard => self.handle_dashboard_key(key, tx, config).await,
            Screen::Form => self.handle_form_key(key, tx, config).await,
            Screen::Confirm => self.handle_confirm_key(key, tx, config).await,
            Screen::Running => {
                if matches!(key.code, KeyCode::Char('q')) {
                    // Defer the exit: killing the process while a tokio task
                    // is mid-flight can leave partial writes (e.g. a batched
                    // record put that has only flushed some batches). Instead
                    // we mark the request and honor it once the action
                    // finishes in `apply_tui_event`.
                    self.exit_after_running = true;
                    self.status_message =
                        "Quit queued. Waiting for the running action to finish...".to_owned();
                    return Ok(true);
                }
                Ok(false)
            }
            Screen::Result => Ok(self.handle_result_key(key)),
        }
    }

    fn handle_home_key(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Char('q') => {
                self.should_exit = true;
                true
            }
            KeyCode::Esc => {
                self.home.search.clear();
                false
            }
            KeyCode::Up => {
                self.move_home_selection(-1);
                false
            }
            KeyCode::Down => {
                self.move_home_selection(1);
                false
            }
            KeyCode::Backspace => {
                self.home.search.pop();
                self.home.selected = 0;
                false
            }
            KeyCode::Char('1') => {
                self.open_concern(ConcernKind::Data);
                false
            }
            KeyCode::Char('2') => {
                self.open_concern(ConcernKind::Collections);
                false
            }
            KeyCode::Char('3') => {
                self.open_concern(ConcernKind::Runtime);
                false
            }
            KeyCode::Char('4') => {
                self.open_concern(ConcernKind::Storage);
                false
            }
            KeyCode::Char('r') if self.result.is_some() => {
                self.screen = Screen::Result;
                false
            }
            KeyCode::Char(character) => {
                if !key.modifiers.contains(KeyModifiers::CONTROL) {
                    self.home.search.push(character);
                    self.home.selected = 0;
                }
                false
            }
            KeyCode::Enter => {
                if let Some(concern) = self.selected_concern() {
                    self.open_concern(concern);
                }
                false
            }
            _ => false,
        }
    }

    async fn handle_dashboard_key(
        &mut self,
        key: KeyEvent,
        tx: UnboundedSender<TuiEvent>,
        config: &LogPoseConfig,
    ) -> anyhow::Result<bool> {
        match key.code {
            KeyCode::Char('q') => {
                self.should_exit = true;
                Ok(true)
            }
            KeyCode::Esc => {
                self.screen = Screen::Home;
                Ok(false)
            }
            KeyCode::Up => {
                self.move_dashboard_selection(-1);
                Ok(false)
            }
            KeyCode::Down => {
                self.move_dashboard_selection(1);
                Ok(false)
            }
            KeyCode::Backspace => {
                self.dashboard.search.pop();
                self.dashboard.selected = 0;
                Ok(false)
            }
            KeyCode::Char('r') if self.result.is_some() => {
                self.screen = Screen::Result;
                Ok(false)
            }
            KeyCode::Char(character) => {
                if !key.modifiers.contains(KeyModifiers::CONTROL) {
                    self.dashboard.search.push(character);
                    self.dashboard.selected = 0;
                }
                Ok(false)
            }
            KeyCode::Enter => {
                let workflow = self.selected_workflow()?;
                let args = InteractiveArgs {
                    workflow: None,
                    create: false,
                    collection: self.session.last_collection.clone(),
                    name: None,
                    dimensions: None,
                    metric: None,
                    input: None,
                    id: None,
                    top_k: None,
                    vector: None,
                    filters: Vec::new(),
                    where_clauses: Vec::new(),
                    predicate_json: None,
                    explain: None,
                    segment_id: None,
                };
                self.open_form(workflow, &args)?;
                if self
                    .form
                    .as_ref()
                    .is_some_and(|form| form.fields.is_empty())
                    && let Some(form) = self.form.take()
                {
                    self.launch_action(form.build_action(&self.cwd)?, tx, config)
                        .await;
                }
                Ok(false)
            }
            _ => Ok(false),
        }
    }

    async fn handle_form_key(
        &mut self,
        key: KeyEvent,
        tx: UnboundedSender<TuiEvent>,
        config: &LogPoseConfig,
    ) -> anyhow::Result<bool> {
        if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('p')) {
            self.open_picker_for_current_field();
            return Ok(false);
        }
        if key.modifiers.contains(KeyModifiers::CONTROL) && matches!(key.code, KeyCode::Char('y')) {
            self.copy_form_command_preview();
            return Ok(false);
        }
        if key.modifiers.contains(KeyModifiers::CONTROL)
            && matches!(key.code, KeyCode::Char('r'))
            && self.result.is_some()
        {
            self.screen = Screen::Result;
            return Ok(false);
        }
        if matches!(key.code, KeyCode::Esc) {
            self.screen = Screen::Dashboard;
            return Ok(false);
        }

        if matches!(key.code, KeyCode::Enter) {
            let Some(form) = self.form.as_mut() else {
                return Ok(false);
            };
            match form.build_action(&self.cwd) {
                Ok(action) => {
                    form.error = None;
                    if let Some(prompt) = confirmation_prompt(&action) {
                        self.confirm = Some(ConfirmState {
                            action,
                            prompt,
                            selected_yes: false,
                        });
                        self.screen = Screen::Confirm;
                    } else {
                        self.launch_action(action, tx, config).await;
                    }
                }
                Err(error) => form.error = Some(error.to_string()),
            }
            return Ok(false);
        }

        if let Some(form) = self.form.as_mut() {
            form.apply_key(key);
            form.error = None;
        }
        Ok(false)
    }

    async fn handle_confirm_key(
        &mut self,
        key: KeyEvent,
        tx: UnboundedSender<TuiEvent>,
        config: &LogPoseConfig,
    ) -> anyhow::Result<bool> {
        let Some(confirm) = self.confirm.as_mut() else {
            return Ok(false);
        };
        match key.code {
            KeyCode::Esc => {
                self.screen = Screen::Form;
                self.confirm = None;
            }
            KeyCode::Char('n') => confirm.selected_yes = false,
            KeyCode::Char('y') => confirm.selected_yes = true,
            KeyCode::Left | KeyCode::Char('h') => confirm.selected_yes = false,
            KeyCode::Right | KeyCode::Char('l') => confirm.selected_yes = true,
            KeyCode::Enter => {
                if confirm.selected_yes {
                    let action = confirm.action.clone();
                    self.confirm = None;
                    self.launch_action(action, tx, config).await;
                } else {
                    self.confirm = None;
                    self.screen = Screen::Form;
                }
            }
            _ => {}
        }
        Ok(false)
    }

    fn handle_result_key(&mut self, key: KeyEvent) -> bool {
        match key.code {
            KeyCode::Char('q') => {
                self.should_exit = true;
                true
            }
            KeyCode::Char('h') => {
                self.screen = Screen::Home;
                false
            }
            KeyCode::Esc | KeyCode::Char('b') => {
                self.return_from_result();
                false
            }
            KeyCode::Char('a') => {
                self.prepare_follow_up_form();
                self.return_from_result();
                self.open_picker_for_current_field();
                false
            }
            KeyCode::Char('y') => {
                self.copy_current_result_view();
                false
            }
            KeyCode::Tab | KeyCode::Right => {
                if let Some(result) = self.result.as_mut() {
                    result.tab = match result.tab {
                        ResultTab::Summary => ResultTab::Json,
                        ResultTab::Json => ResultTab::Command,
                        ResultTab::Command => ResultTab::Summary,
                    };
                }
                false
            }
            KeyCode::BackTab | KeyCode::Left => {
                if let Some(result) = self.result.as_mut() {
                    result.tab = match result.tab {
                        ResultTab::Summary => ResultTab::Command,
                        ResultTab::Json => ResultTab::Summary,
                        ResultTab::Command => ResultTab::Json,
                    };
                }
                false
            }
            KeyCode::Char('1') => self.select_result_tab(ResultTab::Summary),
            KeyCode::Char('2') => self.select_result_tab(ResultTab::Json),
            KeyCode::Char('3') => self.select_result_tab(ResultTab::Command),
            _ => false,
        }
    }

    async fn launch_action(
        &mut self,
        action: Action,
        tx: UnboundedSender<TuiEvent>,
        config: &LogPoseConfig,
    ) {
        self.running = Some(RunningState {
            message: "Preparing action...".to_owned(),
            action: action.clone(),
            frame: 0,
        });
        self.status_message = format!("Running {}…", command_preview(&action));
        self.screen = Screen::Running;
        let config = config.clone();
        tokio::spawn(async move {
            let reporter = ChannelReporter { tx: tx.clone() };
            let result = execute_action(&config, &action, &reporter).await;
            let session = load_session_context(&config).await;
            let _ = tx.send(TuiEvent::ActionComplete {
                action,
                result: Box::new(result),
                session: Box::new(session),
            });
        });
    }

    fn apply_tui_event(&mut self, event: TuiEvent) {
        match event {
            TuiEvent::Progress(progress) => {
                if let Some(running) = self.running.as_mut() {
                    match progress {
                        ProgressEvent::Start(message)
                        | ProgressEvent::Update(message)
                        | ProgressEvent::FinishSuccess(message)
                        | ProgressEvent::FinishInfo(message)
                        | ProgressEvent::Info(message)
                        | ProgressEvent::Warn(message)
                        | ProgressEvent::Error(message) => {
                            running.message = message;
                            self.status_message = running.message.clone();
                        }
                        ProgressEvent::Clear => {}
                    }
                }
            }
            TuiEvent::ActionComplete {
                action,
                result,
                session,
            } => {
                self.session.collections = session.collections.clone();
                self.session.warning = session.warning.clone();
                self.remember_collection(&action);
                match *result {
                    Ok(output) => {
                        let success = success_message(&action);
                        let tab = match self.output_mode {
                            OutputMode::Human => ResultTab::Summary,
                            OutputMode::Json => ResultTab::Json,
                        };
                        self.result = Some(ResultState {
                            action,
                            output,
                            tab,
                        });
                        self.prepare_follow_up_form();
                        self.status_message = success;
                        self.running = None;
                        self.screen = Screen::Result;
                    }
                    Err(error) => {
                        if let Some(form) = self.form.as_mut() {
                            form.error = Some(error.to_string());
                        }
                        self.status_message = error.to_string();
                        self.running = None;
                        self.screen = Screen::Form;
                    }
                }
                if self.exit_after_running {
                    self.should_exit = true;
                }
            }
        }
    }

    fn open_form(&mut self, workflow: WorkflowKind, args: &InteractiveArgs) -> anyhow::Result<()> {
        let form = FormState::from_workflow(workflow, args, &self.cwd, &self.session)?;
        self.form = Some(form);
        self.screen = if self
            .form
            .as_ref()
            .is_some_and(|form| form.fields.is_empty())
        {
            Screen::Dashboard
        } else {
            Screen::Form
        };
        self.picker = None;
        self.dashboard.concern = concern_for_workflow(workflow);
        self.dashboard.selected = 0;
        self.dashboard.search.clear();
        self.status_message = format!(
            "{} ready. Enter runs it, and Ctrl+P opens fuzzy pickers for selectable fields.",
            workflow_title(workflow)
        );
        self.maybe_open_picker_for_current_field();
        Ok(())
    }

    fn open_concern(&mut self, concern: ConcernKind) {
        self.dashboard.concern = concern;
        self.dashboard.search.clear();
        self.dashboard.selected = 0;
        self.home.selected = concern_index(concern).saturating_sub(1);
        self.screen = Screen::Dashboard;
        self.status_message = concern_status_message(concern, &self.session);
    }

    fn remember_collection(&mut self, action: &Action) {
        let collection = match action {
            Action::CollectionCreate(action) => Some(action.name.as_str()),
            Action::CollectionShow(collection)
            | Action::CollectionStats(collection)
            | Action::CollectionPlacement(collection)
            | Action::CollectionFlush(collection)
            | Action::CollectionCompact(collection) => Some(collection.as_str()),
            Action::RecordPut(action) => Some(action.collection.as_str()),
            Action::RecordDelete(action) => Some(action.collection.as_str()),
            Action::Query(action) => Some(action.collection.as_str()),
            Action::Inspect { collection, .. } => Some(collection.as_str()),
            Action::Status | Action::ConfigShow => None,
        };
        self.session.last_collection = collection.map(ToOwned::to_owned);
    }

    fn return_from_result(&mut self) {
        self.screen = if self.form.is_some() {
            Screen::Form
        } else {
            Screen::Dashboard
        };
    }

    fn prepare_follow_up_form(&mut self) {
        let Some(result) = self.result.as_ref() else {
            return;
        };
        if !matches!(result.action, Action::RecordPut(_)) {
            return;
        }
        let Some(form) = self.form.as_mut() else {
            return;
        };
        if let Some(field) = form.field_mut("input") {
            field.value.clear();
        }
        if let Some(index) = form.field_index("input") {
            form.selected = index;
        }
        form.error = None;
    }

    fn select_result_tab(&mut self, tab: ResultTab) -> bool {
        if let Some(result) = self.result.as_mut() {
            result.tab = tab;
        }
        false
    }

    fn current_result_text(&self) -> Option<String> {
        let result = self.result.as_ref()?;
        Some(match result.tab {
            ResultTab::Summary => result.output.human_text().ok()?,
            ResultTab::Json => result.output.json_text().ok()?,
            ResultTab::Command => format_command(&result.action),
        })
    }

    fn copy_current_result_view(&mut self) {
        match self.current_result_text() {
            Some(text) => self.copy_text("result view", &text),
            None => self.status_message = "Nothing is available to copy yet.".to_owned(),
        }
    }

    fn copy_form_command_preview(&mut self) {
        let Some(form) = self.form.as_ref() else {
            return;
        };
        match form.build_action(&self.cwd) {
            Ok(action) => self.copy_text("command preview", &command_preview(&action)),
            Err(error) => self.status_message = error.to_string(),
        }
    }

    fn copy_text(&mut self, label: &str, text: &str) {
        match copy_to_clipboard(text) {
            Ok(()) => self.status_message = format!("Copied {label} to the clipboard."),
            Err(error) => self.status_message = format!("Clipboard copy failed: {error}"),
        }
    }

    fn maybe_open_picker_for_current_field(&mut self) {
        let should_open = self
            .form
            .as_ref()
            .and_then(|form| form.current_field())
            .is_some_and(|field| {
                matches!(field.kind, FieldKind::Collection) && !self.session.collections.is_empty()
            });
        if should_open {
            self.open_picker_for_current_field();
        }
    }

    fn open_picker_for_current_field(&mut self) {
        let Some(form) = self.form.as_ref() else {
            return;
        };
        let Some(field) = form.current_field() else {
            return;
        };
        let picker = match &field.kind {
            FieldKind::Collection => {
                if self.session.collections.is_empty() {
                    None
                } else {
                    let default_index = self
                        .session
                        .collections
                        .iter()
                        .position(|choice| choice.value == field.value)
                        .or_else(|| {
                            self.session.last_collection.as_ref().and_then(|value| {
                                self.session
                                    .collections
                                    .iter()
                                    .position(|choice| choice.value == *value)
                            })
                        })
                        .unwrap_or(0);
                    Some(PickerState {
                        title: "Available Collections".to_owned(),
                        field_key: field.key,
                        query: String::new(),
                        default_index,
                        choices: self.session.collections.clone(),
                        selected: 0,
                        empty_message: "No collections match that search.",
                    })
                }
            }
            FieldKind::Path => {
                let choices = form
                    .file_suggestions
                    .iter()
                    .map(|path| {
                        let label = path
                            .strip_prefix(&self.cwd)
                            .unwrap_or(path)
                            .display()
                            .to_string();
                        picker_choice(
                            path.display().to_string(),
                            &label,
                            "workspace file",
                            &[path
                                .file_name()
                                .and_then(|name| name.to_str())
                                .unwrap_or_default()],
                        )
                    })
                    .collect::<Vec<_>>();
                if choices.is_empty() {
                    None
                } else {
                    Some(PickerState {
                        title: "Choose A File".to_owned(),
                        field_key: field.key,
                        query: String::new(),
                        default_index: 0,
                        choices,
                        selected: 0,
                        empty_message: "No files match that search.",
                    })
                }
            }
            FieldKind::Choice(options) => Some(PickerState {
                title: format!("Choose {}", field.label),
                field_key: field.key,
                query: String::new(),
                default_index: options
                    .iter()
                    .position(|option| *option == field.value)
                    .unwrap_or(0),
                choices: options
                    .iter()
                    .map(|option| picker_choice((*option).to_owned(), option, "option", &[]))
                    .collect(),
                selected: 0,
                empty_message: "No options match that search.",
            }),
            FieldKind::Text | FieldKind::Number => None,
        };
        if let Some(picker) = picker {
            self.status_message = format!(
                "Selecting {}. Type to filter, Enter to choose, Esc to cancel.",
                field.label
            );
            self.picker = Some(picker);
        }
    }

    fn handle_picker_key(&mut self, key: KeyEvent) -> bool {
        let Some(picker) = self.picker.as_mut() else {
            return false;
        };
        match key.code {
            KeyCode::Esc => {
                self.picker = None;
            }
            KeyCode::Up => {
                picker.selected = picker.selected.saturating_sub(1);
            }
            KeyCode::Down => {
                picker.selected = picker.selected.saturating_add(1);
            }
            KeyCode::Backspace => {
                picker.query.pop();
                picker.selected = 0;
            }
            KeyCode::Enter => {
                let filtered = picker.filtered_choices();
                if let Some(choice) = filtered.get(picker.selected) {
                    if let Some(form) = self.form.as_mut()
                        && let Some(field) = form.field_mut(picker.field_key)
                    {
                        field.value = choice.value.clone();
                        if picker.field_key == "collection" {
                            self.session.last_collection = Some(choice.value.clone());
                        }
                        form.error = None;
                    }
                    self.status_message = format!("Selected {}.", choice.label);
                    self.picker = None;
                }
            }
            KeyCode::Char(character) => {
                if !key.modifiers.contains(KeyModifiers::CONTROL) {
                    picker.query.push(character);
                    picker.selected = 0;
                }
            }
            _ => {}
        }
        if let Some(picker) = self.picker.as_mut() {
            let max_index = picker.filtered_choices().len().saturating_sub(1);
            picker.selected = picker.selected.min(max_index);
        }
        false
    }

    fn on_tick(&mut self) {
        if let Some(running) = self.running.as_mut() {
            running.frame = (running.frame + 1) % SPINNER_FRAMES.len();
        }
    }

    fn render(&self, frame: &mut Frame<'_>) {
        frame.render_widget(
            Block::default().style(Style::default().bg(CAT_BASE).fg(CAT_TEXT)),
            frame.area(),
        );

        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(12),
                Constraint::Length(2),
                Constraint::Length(2),
            ])
            .split(frame.area());

        self.render_header(frame, layout[0]);
        match self.screen {
            Screen::Home => self.render_home(frame, layout[1]),
            Screen::Dashboard => self.render_dashboard(frame, layout[1]),
            Screen::Form => self.render_form(frame, layout[1]),
            Screen::Confirm => {
                self.render_form(frame, layout[1]);
                self.render_confirm(frame, layout[1]);
            }
            Screen::Running => {
                if self.form.is_some() {
                    self.render_form(frame, layout[1]);
                } else if self.result.is_some() {
                    self.render_result(frame, layout[1]);
                } else if self.screen == Screen::Running {
                    self.render_dashboard(frame, layout[1]);
                }
                self.render_running(frame, layout[1]);
            }
            Screen::Result => self.render_result(frame, layout[1]),
        }
        self.render_status_bar(frame, layout[2]);
        self.render_shortcuts_bar(frame, layout[3]);

        if self.picker.is_some() {
            self.render_picker(frame, layout[1]);
        }
    }

    fn render_header(&self, frame: &mut Frame<'_>, area: Rect) {
        let title = if self.picker.is_some() {
            "Fuzzy Picker"
        } else {
            match self.screen {
                Screen::Home => "Interactive Home",
                Screen::Dashboard => self
                    .selected_concern_definition()
                    .map(|concern| concern.title)
                    .unwrap_or("Workflow Browser"),
                Screen::Form | Screen::Confirm => self
                    .form
                    .as_ref()
                    .map(|form| form.title.as_str())
                    .unwrap_or("Workflow Form"),
                Screen::Running => "Running Action",
                Screen::Result => self
                    .result
                    .as_ref()
                    .map(|result| result.output.title())
                    .unwrap_or("Result"),
            }
        };

        let mut detail_parts = vec![format!(
            "{} collections visible",
            self.session.collections.len()
        )];
        if let Some(last_collection) = &self.session.last_collection {
            detail_parts.push(format!("last collection: {last_collection}"));
        }
        if self.result.is_some() && self.screen != Screen::Result {
            detail_parts.push("r reopens the latest result".to_owned());
        }

        let header = Paragraph::new(Text::from(vec![
            Line::from(vec![
                Span::styled(
                    " LogPose Interactive ",
                    Style::default()
                        .fg(CAT_BASE)
                        .bg(CAT_BLUE)
                        .add_modifier(Modifier::BOLD),
                ),
                Span::raw(" "),
                Span::styled(
                    title,
                    Style::default()
                        .fg(CAT_LAVENDER)
                        .add_modifier(Modifier::BOLD),
                ),
            ]),
            Line::from(Span::styled(
                detail_parts.join("  |  "),
                Style::default().fg(CAT_SUBTEXT0),
            )),
        ]))
        .block(panel_block("Session"))
        .wrap(Wrap { trim: true });
        frame.render_widget(header, area);
    }

    fn render_home(&self, frame: &mut Frame<'_>, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Min(8)])
            .split(area);
        frame.render_widget(
            Paragraph::new(self.home.search.as_str())
                .block(panel_block("Filter Areas"))
                .style(Style::default().fg(CAT_TEXT)),
            layout[0],
        );

        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(46), Constraint::Percentage(54)])
            .split(layout[1]);

        let concerns = self.filtered_concerns();
        let selected_index = self.home.selected.min(concerns.len().saturating_sub(1));
        let items = if concerns.is_empty() {
            vec![ListItem::new(Line::from(Span::styled(
                "No areas match that search.",
                Style::default().fg(CAT_SUBTEXT0),
            )))]
        } else {
            concerns
                .iter()
                .enumerate()
                .map(|(index, kind)| {
                    let concern = self
                        .concern_definition(*kind)
                        .expect("concern definitions stay in sync");
                    let style = if index == selected_index {
                        Style::default()
                            .fg(CAT_BASE)
                            .bg(CAT_MAUVE)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default().fg(CAT_TEXT)
                    };
                    ListItem::new(Line::from(vec![
                        Span::styled(
                            format!("[{}] ", concern_index(concern.kind)),
                            Style::default().fg(CAT_PEACH),
                        ),
                        Span::styled(concern.title, style),
                        Span::raw("  "),
                        Span::styled(concern.detail, Style::default().fg(CAT_SUBTEXT0)),
                    ]))
                })
                .collect()
        };
        frame.render_widget(
            List::new(items)
                .block(panel_block("Areas"))
                .highlight_style(
                    Style::default()
                        .fg(CAT_BASE)
                        .bg(CAT_MAUVE)
                        .add_modifier(Modifier::BOLD),
                ),
            body[0],
        );

        let preview_lines = if let Some(kind) = concerns.get(selected_index) {
            let concern = self
                .concern_definition(*kind)
                .expect("concern definitions stay in sync");
            let mut lines = vec![
                Line::from(Span::styled(concern.title, section_title_style(CAT_BLUE))),
                Line::from(concern.detail),
                Line::raw(""),
                Line::from(Span::styled(
                    "Common workflows",
                    section_title_style(CAT_GREEN),
                )),
            ];
            lines.extend(concern.workflows.iter().filter_map(|workflow| {
                self.workflow_definition(*workflow).map(|definition| {
                    Line::from(format!("- {}: {}", definition.label, definition.detail))
                })
            }));
            if let Some(warning) = &self.session.warning {
                lines.push(Line::raw(""));
                lines.push(Line::from(Span::styled(
                    "Runtime note",
                    section_title_style(CAT_YELLOW),
                )));
                lines.push(Line::from(warning.clone()));
            }
            lines
        } else {
            vec![Line::from(Span::styled(
                "Type to filter the available areas.",
                Style::default().fg(CAT_SUBTEXT0),
            ))]
        };
        frame.render_widget(
            Paragraph::new(Text::from(preview_lines))
                .block(panel_block("Preview"))
                .wrap(Wrap { trim: true }),
            body[1],
        );
    }

    fn render_dashboard(&self, frame: &mut Frame<'_>, area: Rect) {
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Min(8)])
            .split(area);

        let concern_title = self
            .selected_concern_definition()
            .map(|concern| concern.title)
            .unwrap_or("Workflows");
        frame.render_widget(
            Paragraph::new(self.dashboard.search.as_str())
                .block(panel_block(&format!("{concern_title} Search")))
                .style(Style::default().fg(CAT_TEXT)),
            layout[0],
        );

        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(48), Constraint::Percentage(52)])
            .split(layout[1]);

        let workflows = self.filtered_workflows();
        let selected_index = self
            .dashboard
            .selected
            .min(workflows.len().saturating_sub(1));
        let items = if workflows.is_empty() {
            vec![ListItem::new(Line::from(Span::styled(
                "No workflows match that search.",
                Style::default().fg(CAT_SUBTEXT0),
            )))]
        } else {
            workflows
                .iter()
                .enumerate()
                .filter_map(|(index, workflow)| {
                    self.workflow_definition(*workflow).map(|definition| {
                        let style = if index == selected_index {
                            Style::default()
                                .fg(CAT_BASE)
                                .bg(CAT_BLUE)
                                .add_modifier(Modifier::BOLD)
                        } else {
                            Style::default().fg(CAT_TEXT)
                        };
                        ListItem::new(Line::from(vec![
                            Span::styled(definition.label, style.add_modifier(Modifier::BOLD)),
                            Span::raw("  "),
                            Span::styled(definition.detail, Style::default().fg(CAT_SUBTEXT0)),
                        ]))
                    })
                })
                .collect()
        };
        frame.render_widget(List::new(items).block(panel_block("Workflows")), body[0]);

        let detail_lines = if let Some(workflow) = workflows.get(selected_index) {
            let definition = self
                .workflow_definition(*workflow)
                .expect("workflow definitions stay in sync");
            let mut lines = vec![
                Line::from(Span::styled(
                    definition.label,
                    section_title_style(CAT_MAUVE),
                )),
                Line::from(definition.detail),
                Line::raw(""),
                Line::from(Span::styled(
                    "What opens next",
                    section_title_style(CAT_GREEN),
                )),
                Line::from(match workflow {
                    WorkflowKind::Status | WorkflowKind::ConfigShow => {
                        "This action runs immediately and keeps the result view open.".to_owned()
                    }
                    _ => "A guided form opens first, with fuzzy pickers for selectable fields."
                        .to_owned(),
                }),
            ];
            if workflow_uses_collection(*workflow) {
                lines.push(Line::raw(""));
                lines.push(Line::from(Span::styled(
                    "Available collections",
                    section_title_style(CAT_BLUE),
                )));
                if self.session.collections.is_empty() {
                    lines.push(Line::from(
                        "Collection suggestions will appear here after runtime status succeeds.",
                    ));
                } else {
                    lines.extend(self.session.collections.iter().take(8).map(|choice| {
                        Line::from(format!("- {}: {}", choice.label, choice.detail))
                    }));
                }
            }
            lines
        } else {
            vec![Line::from(Span::styled(
                "Start typing to narrow the workflows in this area.",
                Style::default().fg(CAT_SUBTEXT0),
            ))]
        };
        frame.render_widget(
            Paragraph::new(Text::from(detail_lines))
                .block(panel_block("Workflow Preview"))
                .wrap(Wrap { trim: true }),
            body[1],
        );
    }

    fn render_form(&self, frame: &mut Frame<'_>, area: Rect) {
        let Some(form) = self.form.as_ref() else {
            return;
        };
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Min(8)])
            .split(area);
        frame.render_widget(
            Paragraph::new(form.description.as_str())
                .block(panel_block(form.title.as_str()))
                .style(Style::default().fg(CAT_TEXT)),
            layout[0],
        );

        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(56), Constraint::Percentage(44)])
            .split(layout[1]);
        let selected_index = form.selected.min(form.fields.len().saturating_sub(1));
        let fields = if form.fields.is_empty() {
            vec![ListItem::new(Line::from(
                "No form fields are required for this workflow.",
            ))]
        } else {
            form.fields
                .iter()
                .enumerate()
                .map(|(index, field)| {
                    let value_style = if field.value.is_empty() {
                        Style::default()
                            .fg(CAT_SUBTEXT0)
                            .add_modifier(Modifier::ITALIC)
                    } else {
                        Style::default().fg(CAT_TEXT)
                    };
                    let selected = index == selected_index;
                    let prefix = if selected { ">" } else { " " };
                    let kind_badge = match &field.kind {
                        FieldKind::Text => "text",
                        FieldKind::Number => "number",
                        FieldKind::Path => "file",
                        FieldKind::Collection => "collection",
                        FieldKind::Choice(_) => "select",
                    };
                    let label = if field.required {
                        format!("{prefix} {} *", field.label)
                    } else {
                        format!("{prefix} {}", field.label)
                    };
                    let value = if field.value.is_empty() {
                        format!("<{}>", field.placeholder)
                    } else {
                        field.value.clone()
                    };
                    let style = if selected {
                        Style::default()
                            .fg(CAT_BASE)
                            .bg(CAT_SKY)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default().fg(CAT_TEXT)
                    };
                    ListItem::new(Line::from(vec![
                        Span::styled(label, style),
                        Span::raw("  "),
                        Span::styled(format!("[{kind_badge}]"), Style::default().fg(CAT_PEACH)),
                        Span::raw("  "),
                        Span::styled(value, if selected { style } else { value_style }),
                    ]))
                })
                .collect()
        };
        let field_block = if form.error.is_some() {
            error_block("Guided Form")
        } else {
            panel_block("Guided Form")
        };
        frame.render_widget(List::new(fields).block(field_block), body[0]);

        let mut preview_lines = vec![
            Line::from(Span::styled("Exact command", section_title_style(CAT_BLUE))),
            Line::from(match form.build_action(&self.cwd) {
                Ok(action) => command_preview(&action),
                Err(_) => "Complete the required fields to preview the exact command.".to_owned(),
            }),
            Line::raw(""),
        ];
        if let Some(field) = form.current_field() {
            preview_lines.push(Line::from(Span::styled(
                field.label,
                section_title_style(CAT_GREEN),
            )));
            preview_lines.push(Line::from(field.help));
            preview_lines.push(Line::from(if field.required {
                "Required field"
            } else {
                "Optional field"
            }));
            preview_lines.push(Line::raw(""));
            preview_lines.push(Line::from(Span::styled(
                "Suggestions",
                section_title_style(CAT_MAUVE),
            )));
            preview_lines.extend(self.field_suggestion_lines(form, field));
        }
        frame.render_widget(
            Paragraph::new(Text::from(preview_lines))
                .block(panel_block("Preview"))
                .wrap(Wrap { trim: true }),
            body[1],
        );
    }

    fn render_confirm(&self, frame: &mut Frame<'_>, area: Rect) {
        let Some(confirm) = self.confirm.as_ref() else {
            return;
        };
        let overlay = centered_rect(62, 34, area);
        frame.render_widget(Clear, overlay);
        let lines = vec![
            Line::from(Span::styled(
                confirm.prompt.as_str(),
                section_title_style(CAT_YELLOW),
            )),
            Line::raw(""),
            Line::from(command_preview(&confirm.action)),
            Line::raw(""),
            Line::from(vec![
                Span::styled(
                    "[n] No",
                    if confirm.selected_yes {
                        Style::default().fg(CAT_SUBTEXT0)
                    } else {
                        Style::default()
                            .fg(CAT_BASE)
                            .bg(CAT_YELLOW)
                            .add_modifier(Modifier::BOLD)
                    },
                ),
                Span::raw("   "),
                Span::styled(
                    "[y] Yes",
                    if confirm.selected_yes {
                        Style::default()
                            .fg(CAT_BASE)
                            .bg(CAT_GREEN)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default().fg(CAT_SUBTEXT0)
                    },
                ),
            ]),
        ];
        frame.render_widget(
            Paragraph::new(Text::from(lines))
                .block(panel_block("Confirm Action"))
                .wrap(Wrap { trim: true }),
            overlay,
        );
    }

    fn render_running(&self, frame: &mut Frame<'_>, area: Rect) {
        let Some(running) = self.running.as_ref() else {
            return;
        };
        let overlay = centered_rect(70, 42, area);
        frame.render_widget(Clear, overlay);
        let content = vec![
            Line::from(Span::styled(
                format!("{} {}", SPINNER_FRAMES[running.frame], running.message),
                section_title_style(CAT_SKY),
            )),
            Line::raw(""),
            Line::from(Span::styled("Exact command", section_title_style(CAT_BLUE))),
            Line::from(command_preview(&running.action)),
            Line::raw(""),
            Line::from(
                "The result view stays open when this finishes so you can copy or keep browsing.",
            ),
        ];
        frame.render_widget(
            Paragraph::new(Text::from(content))
                .block(panel_block("Working"))
                .wrap(Wrap { trim: true }),
            overlay,
        );
    }

    fn render_result(&self, frame: &mut Frame<'_>, area: Rect) {
        let Some(result) = self.result.as_ref() else {
            return;
        };
        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(72), Constraint::Percentage(28)])
            .split(area);

        let left = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Min(8)])
            .split(body[0]);
        let tabs = Tabs::new(vec!["Summary", "JSON", "Command"])
            .select(match result.tab {
                ResultTab::Summary => 0,
                ResultTab::Json => 1,
                ResultTab::Command => 2,
            })
            .block(panel_block("Views"))
            .style(Style::default().fg(CAT_SUBTEXT0))
            .highlight_style(
                Style::default()
                    .fg(CAT_BASE)
                    .bg(CAT_BLUE)
                    .add_modifier(Modifier::BOLD),
            );
        frame.render_widget(tabs, left[0]);

        let text = match result.tab {
            ResultTab::Summary => result
                .output
                .human_text()
                .unwrap_or_else(|error| error.to_string()),
            ResultTab::Json => result
                .output
                .json_text()
                .unwrap_or_else(|error| error.to_string()),
            ResultTab::Command => format_command(&result.action),
        };
        frame.render_widget(
            Paragraph::new(text)
                .block(panel_block("Result Body"))
                .style(Style::default().fg(CAT_TEXT))
                .wrap(Wrap { trim: false }),
            left[1],
        );

        let mut side_lines = vec![
            Line::from(Span::styled(
                result.output.title(),
                section_title_style(CAT_GREEN),
            )),
            Line::raw(""),
            Line::from(Span::styled("Action", section_title_style(CAT_BLUE))),
            Line::from(command_preview(&result.action)),
        ];
        if matches!(result.action, Action::RecordPut(_)) {
            side_lines.push(Line::raw(""));
            side_lines.push(Line::from(Span::styled(
                "Fast follow-up",
                section_title_style(CAT_PEACH),
            )));
            side_lines.push(Line::from(
                "Press a to keep the collection and pick another file.",
            ));
        }
        side_lines.push(Line::raw(""));
        side_lines.push(Line::from(Span::styled(
            "Copy",
            section_title_style(CAT_MAUVE),
        )));
        side_lines.push(Line::from(
            "Press y to copy the current tab to the clipboard.",
        ));
        frame.render_widget(
            Paragraph::new(Text::from(side_lines))
                .block(panel_block("Next Step"))
                .wrap(Wrap { trim: true }),
            body[1],
        );
    }

    fn render_picker(&self, frame: &mut Frame<'_>, area: Rect) {
        let Some(picker) = self.picker.as_ref() else {
            return;
        };
        let overlay = centered_rect(72, 70, area);
        frame.render_widget(Clear, overlay);
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),
                Constraint::Min(8),
                Constraint::Length(2),
            ])
            .split(overlay);
        frame.render_widget(
            Paragraph::new(picker.query.as_str())
                .block(panel_block(&picker.title))
                .style(Style::default().fg(CAT_TEXT)),
            layout[0],
        );

        let body = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Percentage(58), Constraint::Percentage(42)])
            .split(layout[1]);
        let filtered = picker.filtered_choices();
        let selected_index = picker.selected.min(filtered.len().saturating_sub(1));
        let items = if filtered.is_empty() {
            vec![ListItem::new(Line::from(Span::styled(
                picker.empty_message,
                Style::default().fg(CAT_SUBTEXT0),
            )))]
        } else {
            filtered
                .iter()
                .enumerate()
                .map(|(index, choice)| {
                    let style = if index == selected_index {
                        Style::default()
                            .fg(CAT_BASE)
                            .bg(CAT_GREEN)
                            .add_modifier(Modifier::BOLD)
                    } else {
                        Style::default().fg(CAT_TEXT)
                    };
                    ListItem::new(Line::from(vec![
                        Span::styled(choice.label.as_str(), style),
                        Span::raw("  "),
                        Span::styled(choice.detail.as_str(), Style::default().fg(CAT_SUBTEXT0)),
                    ]))
                })
                .collect()
        };
        frame.render_widget(List::new(items).block(panel_block("Matches")), body[0]);

        let detail_lines = if let Some(choice) = filtered.get(selected_index) {
            vec![
                Line::from(Span::styled(
                    choice.label.as_str(),
                    section_title_style(CAT_BLUE),
                )),
                Line::from(choice.detail.as_str()),
                Line::raw(""),
                Line::from("Fuzzy matching uses the label, detail text, and aliases."),
            ]
        } else {
            vec![Line::from("Type to narrow the choices.")]
        };
        frame.render_widget(
            Paragraph::new(Text::from(detail_lines))
                .block(panel_block("Selected Match"))
                .wrap(Wrap { trim: true }),
            body[1],
        );
        frame.render_widget(
            Paragraph::new("Enter choose  |  Esc cancel  |  Type to filter  |  Up/Down move")
                .block(panel_block("Picker Keys"))
                .style(Style::default().fg(CAT_SUBTEXT0)),
            layout[2],
        );
    }

    fn render_status_bar(&self, frame: &mut Frame<'_>, area: Rect) {
        let (message, style) = self.current_status_line();
        frame.render_widget(
            Paragraph::new(message)
                .style(style)
                .block(
                    Block::default()
                        .borders(Borders::TOP)
                        .border_style(Style::default().fg(CAT_SURFACE2))
                        .style(Style::default().bg(CAT_MANTLE)),
                )
                .wrap(Wrap { trim: true }),
            area,
        );
    }

    fn render_shortcuts_bar(&self, frame: &mut Frame<'_>, area: Rect) {
        frame.render_widget(
            Paragraph::new(self.shortcut_line())
                .style(
                    Style::default()
                        .fg(CAT_BASE)
                        .bg(CAT_LAVENDER)
                        .add_modifier(Modifier::BOLD),
                )
                .block(Block::default()),
            area,
        );
    }

    fn current_status_line(&self) -> (String, Style) {
        if let Some(error) = self.form.as_ref().and_then(|form| form.error.as_ref()) {
            return (error.clone(), Style::default().fg(CAT_RED).bg(CAT_MANTLE));
        }
        if let Some(warning) = &self.session.warning
            && self.status_message == "Interactive workspace ready. Pick an area to begin."
        {
            return (
                warning.clone(),
                Style::default().fg(CAT_YELLOW).bg(CAT_MANTLE),
            );
        }
        (
            self.status_message.clone(),
            Style::default().fg(CAT_SUBTEXT1).bg(CAT_MANTLE),
        )
    }

    fn shortcut_line(&self) -> String {
        if self.picker.is_some() {
            return "Type filter  Enter choose  Esc cancel  Up/Down move".to_owned();
        }
        match self.screen {
            Screen::Home => {
                "Enter open area  1-4 jump  type filter  r reopen result  q quit".to_owned()
            }
            Screen::Dashboard => {
                "Enter open workflow  Esc home  type filter  Up/Down move  r reopen result  q quit"
                    .to_owned()
            }
            Screen::Form => {
                "Enter run  Tab move  Left/Right cycle  Ctrl+P fuzzy pick  Ctrl+Y copy cmd  Esc back"
                    .to_owned()
            }
            Screen::Confirm => {
                "Left/Right or y/n choose  Enter confirm  Esc cancel".to_owned()
            }
            Screen::Running => "q quit session after the current render loop tick".to_owned(),
            Screen::Result => {
                let follow_up = if self
                    .result
                    .as_ref()
                    .is_some_and(|result| matches!(result.action, Action::RecordPut(_)))
                {
                    "  a add another file"
                } else {
                    ""
                };
                format!(
                    "1/2/3 switch view  y copy  Esc back  h home{follow_up}  q quit"
                )
            }
        }
    }

    fn field_suggestion_lines(&self, form: &FormState, field: &FormField) -> Vec<Line<'static>> {
        match &field.kind {
            FieldKind::Collection => {
                if self.session.collections.is_empty() {
                    vec![Line::from(
                        "No collections are cached yet. Connect to the runtime or type a name.",
                    )]
                } else {
                    rank_picker_choices(&self.session.collections, &field.value, 0)
                        .into_iter()
                        .take(6)
                        .map(|choice| Line::from(format!("- {}: {}", choice.label, choice.detail)))
                        .chain(std::iter::once(Line::raw("")))
                        .chain(std::iter::once(Line::from(
                            "Ctrl+P opens the fuzzy collection picker.",
                        )))
                        .collect()
                }
            }
            FieldKind::Path => {
                if form.file_suggestions.is_empty() {
                    vec![Line::from(
                        "No workspace files were indexed. Type a path or use an absolute path.",
                    )]
                } else {
                    rank_path_choices(&form.file_suggestions, &self.cwd, &field.value)
                        .into_iter()
                        .take(6)
                        .map(|choice| Line::from(format!("- {}", choice.display)))
                        .chain(std::iter::once(Line::raw("")))
                        .chain(std::iter::once(Line::from(
                            "Ctrl+P opens the fuzzy file picker.",
                        )))
                        .collect()
                }
            }
            FieldKind::Choice(options) => options
                .iter()
                .map(|option| {
                    if *option == field.value {
                        Line::from(format!("- {option} (selected)"))
                    } else {
                        Line::from(format!("- {option}"))
                    }
                })
                .chain(std::iter::once(Line::raw("")))
                .chain(std::iter::once(Line::from(
                    "Use Left/Right to cycle or Ctrl+P to fuzzy search the options.",
                )))
                .collect(),
            FieldKind::Text => vec![Line::from("Type directly into this field.")],
            FieldKind::Number => vec![Line::from("Type digits only into this field.")],
        }
    }

    fn filtered_concerns(&self) -> Vec<ConcernKind> {
        let choices = self
            .concerns
            .iter()
            .map(|concern| {
                picker_choice(concern.kind, concern.title, concern.detail, concern.aliases)
            })
            .collect::<Vec<_>>();
        rank_picker_choices(&choices, &self.home.search, 0)
            .into_iter()
            .map(|choice| choice.value)
            .collect()
    }

    fn concern_definition(&self, kind: ConcernKind) -> Option<&ConcernDefinition> {
        self.concerns.iter().find(|concern| concern.kind == kind)
    }

    fn selected_concern_definition(&self) -> Option<&ConcernDefinition> {
        let concerns = self.filtered_concerns();
        let selected_index = self.home.selected.min(concerns.len().saturating_sub(1));
        concerns
            .get(selected_index)
            .and_then(|kind| self.concern_definition(*kind))
            .or_else(|| self.concern_definition(self.dashboard.concern))
    }

    fn selected_concern(&self) -> Option<ConcernKind> {
        let concerns = self.filtered_concerns();
        concerns
            .get(self.home.selected.min(concerns.len().saturating_sub(1)))
            .copied()
    }

    fn move_home_selection(&mut self, delta: isize) {
        let len = self.filtered_concerns().len();
        self.home.selected = move_index(self.home.selected, len, delta);
    }

    fn filtered_workflows(&self) -> Vec<WorkflowKind> {
        let workflows = self
            .concern_definition(self.dashboard.concern)
            .map(|concern| concern.workflows)
            .unwrap_or_default();
        let choices = workflows
            .iter()
            .filter_map(|workflow| {
                self.workflow_definition(*workflow).map(|definition| {
                    picker_choice(
                        definition.kind,
                        definition.label,
                        definition.detail,
                        definition.aliases,
                    )
                })
            })
            .collect::<Vec<_>>();
        rank_picker_choices(&choices, &self.dashboard.search, 0)
            .into_iter()
            .map(|choice| choice.value)
            .collect()
    }

    fn workflow_definition(&self, workflow: WorkflowKind) -> Option<&WorkflowDefinition> {
        self.definitions
            .iter()
            .find(|definition| definition.kind == workflow)
    }

    fn move_dashboard_selection(&mut self, delta: isize) {
        let len = self.filtered_workflows().len();
        self.dashboard.selected = move_index(self.dashboard.selected, len, delta);
    }

    fn selected_workflow(&self) -> anyhow::Result<WorkflowKind> {
        let filtered = self.filtered_workflows();
        filtered
            .get(
                self.dashboard
                    .selected
                    .min(filtered.len().saturating_sub(1)),
            )
            .copied()
            .ok_or_else(|| anyhow::anyhow!("no workflow matches the current search"))
    }
}

const SPINNER_FRAMES: [&str; 10] = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

fn workflow_title(workflow: WorkflowKind) -> &'static str {
    match workflow {
        WorkflowKind::CollectionCreate => "Create Collection",
        WorkflowKind::CollectionShow => "Show Collection",
        WorkflowKind::CollectionStats => "Collection Stats",
        WorkflowKind::CollectionPlacement => "Collection Placement",
        WorkflowKind::CollectionFlush => "Flush Collection",
        WorkflowKind::CollectionCompact => "Compact Collection",
        WorkflowKind::RecordPut => "Ingest Records",
        WorkflowKind::RecordDelete => "Delete Record",
        WorkflowKind::Query => "Run Query",
        WorkflowKind::InspectManifest => "Inspect Manifest",
        WorkflowKind::InspectWal => "Inspect WAL",
        WorkflowKind::InspectMaintenance => "Inspect Maintenance",
        WorkflowKind::InspectSegment => "Inspect Segment",
        WorkflowKind::Status => "Runtime Status",
        WorkflowKind::ConfigShow => "Show Configuration",
    }
}

fn workflow_description(workflow: WorkflowKind) -> &'static str {
    match workflow {
        WorkflowKind::CollectionCreate => "Define a new collection with dimensions and metric.",
        WorkflowKind::CollectionShow => "Read collection metadata.",
        WorkflowKind::CollectionStats => "Inspect collection storage and planner statistics.",
        WorkflowKind::CollectionPlacement => "Explain the current placement decision.",
        WorkflowKind::CollectionFlush => "Promote mutable data into an immutable segment.",
        WorkflowKind::CollectionCompact => "Compact immutable segments into a replacement segment.",
        WorkflowKind::RecordPut => "Load records from JSONL into a collection.",
        WorkflowKind::RecordDelete => "Delete a single record id from a collection.",
        WorkflowKind::Query => "Run vector search with optional filters and diagnostics.",
        WorkflowKind::InspectManifest => "Inspect the active manifest payload.",
        WorkflowKind::InspectWal => "Inspect WAL records above the durable checkpoint.",
        WorkflowKind::InspectMaintenance => "Inspect persisted maintenance state.",
        WorkflowKind::InspectSegment => "Inspect one immutable segment by id.",
        WorkflowKind::Status => "Read runtime status and endpoint details.",
        WorkflowKind::ConfigShow => "Show the effective node configuration.",
    }
}

fn confirmation_prompt(action: &Action) -> Option<String> {
    match action {
        Action::CollectionFlush(collection) => {
            Some(format!("Flush collection '{collection}' now?"))
        }
        Action::CollectionCompact(collection) => {
            Some(format!("Compact collection '{collection}' now?"))
        }
        Action::RecordDelete(action) => Some(format!(
            "Delete record '{}' from collection '{}' now?",
            action.id, action.collection
        )),
        _ => None,
    }
}

fn text_field(
    key: &'static str,
    label: &'static str,
    help: &'static str,
    placeholder: &'static str,
    required: bool,
    value: Option<String>,
) -> FormField {
    FormField {
        key,
        label,
        help,
        placeholder,
        required,
        kind: FieldKind::Text,
        value: value.unwrap_or_default(),
    }
}

fn number_field(
    key: &'static str,
    label: &'static str,
    help: &'static str,
    placeholder: &'static str,
    required: bool,
    value: Option<String>,
) -> FormField {
    FormField {
        key,
        label,
        help,
        placeholder,
        required,
        kind: FieldKind::Number,
        value: value.unwrap_or_default(),
    }
}

fn path_field(
    key: &'static str,
    label: &'static str,
    help: &'static str,
    placeholder: &'static str,
    required: bool,
    value: Option<String>,
) -> FormField {
    FormField {
        key,
        label,
        help,
        placeholder,
        required,
        kind: FieldKind::Path,
        value: value.unwrap_or_default(),
    }
}

fn choice_field(
    key: &'static str,
    label: &'static str,
    help: &'static str,
    options: Vec<&'static str>,
    value: Option<&'static str>,
    default: &'static str,
) -> FormField {
    FormField {
        key,
        label,
        help,
        placeholder: default,
        required: true,
        kind: FieldKind::Choice(options),
        value: value.unwrap_or(default).to_owned(),
    }
}

fn collection_field(
    key: &'static str,
    label: &'static str,
    help: &'static str,
    placeholder: &'static str,
    required: bool,
    value: Option<String>,
    session: &SessionContext,
) -> FormField {
    let value = value
        .filter(|value| !value.trim().is_empty())
        .or_else(|| session.last_collection.clone())
        .or_else(|| {
            session
                .collections
                .first()
                .map(|choice| choice.value.clone())
        })
        .unwrap_or_default();
    FormField {
        key,
        label,
        help,
        placeholder,
        required,
        kind: FieldKind::Collection,
        value,
    }
}

fn metric_to_value(metric: MetricArg) -> &'static str {
    match metric {
        MetricArg::Cosine => "cosine",
        MetricArg::Dot => "dot",
        MetricArg::L2 => "l2",
    }
}

fn explain_to_value(explain: ExplainArg) -> &'static str {
    match explain {
        ExplainArg::Plan => "plan",
        ExplainArg::Profile => "profile",
    }
}

fn vector_to_value(vector: &crate::action::QueryVector) -> String {
    vector
        .0
        .iter()
        .map(|component| format!("{component}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn filter_to_value(filter: &crate::action::QueryFilter) -> String {
    // Build the literal directly from the typed filter instead of
    // round-tripping through shell-escaped command text, because that
    // round-trip corrupts values that contain apostrophes (e.g. `O'Brien`
    // becomes `O'"'"'Brien`) and would silently change query semantics when
    // the form is pre-filled.
    format_filter(filter)
}

fn predicate_to_value(predicate: &Predicate) -> String {
    // Same reasoning as `filter_to_value`: format the predicate literal
    // directly so apostrophes and other shell-significant characters
    // survive round-tripping into the TUI form.
    format_predicate(predicate)
}

fn parse_metric(value: &str) -> anyhow::Result<MetricArg> {
    match value {
        "cosine" => Ok(MetricArg::Cosine),
        "dot" => Ok(MetricArg::Dot),
        "l2" => Ok(MetricArg::L2),
        _ => bail!("unsupported metric '{value}'"),
    }
}

fn parse_explain(value: &str) -> anyhow::Result<Option<ExplainArg>> {
    match value {
        "" | "none" => Ok(None),
        "plan" => Ok(Some(ExplainArg::Plan)),
        "profile" => Ok(Some(ExplainArg::Profile)),
        _ => bail!("unsupported explain mode '{value}'"),
    }
}

fn parse_optional_filters(value: &str) -> anyhow::Result<Vec<crate::action::QueryFilter>> {
    if value.trim().is_empty() {
        Ok(Vec::new())
    } else {
        parse_filter_list(value).map_err(anyhow::Error::msg)
    }
}

fn parse_optional_predicates(value: &str) -> anyhow::Result<Vec<Predicate>> {
    if value.trim().is_empty() {
        Ok(Vec::new())
    } else {
        parse_where_list(value).map_err(anyhow::Error::msg)
    }
}

/// Resolves a user-entered path while preserving the typed value.
///
/// Relative paths are joined to the current working directory so that
/// downstream consumers see an unambiguous location, but the file component
/// is never replaced with a fuzzy match. This ensures a typo like
/// `records.jonl` fails with a clear "file not found" rather than silently
/// ingesting a different existing file.
fn resolve_user_path(cwd: &Path, value: &str) -> PathBuf {
    let direct = PathBuf::from(value);
    if direct.is_absolute() {
        direct
    } else {
        cwd.join(direct)
    }
}

fn centered_rect(percent_x: u16, percent_y: u16, rect: Rect) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(rect);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
}

const CAT_BASE: Color = Color::Rgb(30, 30, 46);
const CAT_MANTLE: Color = Color::Rgb(24, 24, 37);
const CAT_SURFACE2: Color = Color::Rgb(88, 91, 112);
const CAT_TEXT: Color = Color::Rgb(205, 214, 244);
const CAT_SUBTEXT0: Color = Color::Rgb(166, 173, 200);
const CAT_SUBTEXT1: Color = Color::Rgb(186, 194, 222);
const CAT_BLUE: Color = Color::Rgb(137, 180, 250);
const CAT_GREEN: Color = Color::Rgb(166, 227, 161);
const CAT_YELLOW: Color = Color::Rgb(249, 226, 175);
const CAT_PEACH: Color = Color::Rgb(250, 179, 135);
const CAT_RED: Color = Color::Rgb(243, 139, 168);
const CAT_MAUVE: Color = Color::Rgb(203, 166, 247);
const CAT_SKY: Color = Color::Rgb(137, 220, 235);
const CAT_LAVENDER: Color = Color::Rgb(180, 190, 254);

const DATA_WORKFLOWS: &[WorkflowKind] = &[
    WorkflowKind::Query,
    WorkflowKind::RecordPut,
    WorkflowKind::RecordDelete,
];
const COLLECTION_WORKFLOWS: &[WorkflowKind] = &[
    WorkflowKind::CollectionShow,
    WorkflowKind::CollectionStats,
    WorkflowKind::CollectionPlacement,
    WorkflowKind::CollectionCreate,
];
const RUNTIME_WORKFLOWS: &[WorkflowKind] = &[WorkflowKind::Status, WorkflowKind::ConfigShow];
const STORAGE_WORKFLOWS: &[WorkflowKind] = &[
    WorkflowKind::CollectionFlush,
    WorkflowKind::CollectionCompact,
    WorkflowKind::InspectManifest,
    WorkflowKind::InspectWal,
    WorkflowKind::InspectMaintenance,
    WorkflowKind::InspectSegment,
];

fn concern_definitions() -> Vec<ConcernDefinition> {
    vec![
        ConcernDefinition {
            kind: ConcernKind::Data,
            title: "Data And Query",
            detail: "Run searches, ingest files, and clean up records.",
            aliases: &["data", "query", "search", "records", "ingest"],
            workflows: DATA_WORKFLOWS,
        },
        ConcernDefinition {
            kind: ConcernKind::Collections,
            title: "Collections",
            detail: "Browse metadata, statistics, routing, and creation workflows.",
            aliases: &["collections", "catalog", "metadata", "placement"],
            workflows: COLLECTION_WORKFLOWS,
        },
        ConcernDefinition {
            kind: ConcernKind::Runtime,
            title: "Runtime And Cluster",
            detail: "Check runtime health, readiness, endpoints, and effective config.",
            aliases: &["runtime", "cluster", "health", "config", "status"],
            workflows: RUNTIME_WORKFLOWS,
        },
        ConcernDefinition {
            kind: ConcernKind::Storage,
            title: "Maintenance And Inspect",
            detail: "Inspect storage internals and run heavier maintenance operations.",
            aliases: &["maintenance", "inspect", "storage", "wal", "manifest"],
            workflows: STORAGE_WORKFLOWS,
        },
    ]
}

fn concern_for_workflow(workflow: WorkflowKind) -> ConcernKind {
    match workflow {
        WorkflowKind::Query | WorkflowKind::RecordPut | WorkflowKind::RecordDelete => {
            ConcernKind::Data
        }
        WorkflowKind::CollectionCreate
        | WorkflowKind::CollectionShow
        | WorkflowKind::CollectionStats
        | WorkflowKind::CollectionPlacement => ConcernKind::Collections,
        WorkflowKind::Status | WorkflowKind::ConfigShow => ConcernKind::Runtime,
        WorkflowKind::CollectionFlush
        | WorkflowKind::CollectionCompact
        | WorkflowKind::InspectManifest
        | WorkflowKind::InspectWal
        | WorkflowKind::InspectMaintenance
        | WorkflowKind::InspectSegment => ConcernKind::Storage,
    }
}

fn concern_status_message(concern: ConcernKind, session: &SessionContext) -> String {
    let collection_hint = if session.collections.is_empty() {
        "Collection suggestions will appear once runtime status is available.".to_owned()
    } else {
        format!(
            "{} collections are ready for fuzzy selection.",
            session.collections.len()
        )
    };
    match concern {
        ConcernKind::Data => {
            format!("Data workflows ready. {collection_hint}")
        }
        ConcernKind::Collections => {
            format!("Collection workflows ready. {collection_hint}")
        }
        ConcernKind::Runtime => {
            "Runtime checks are ready. Status and config can run immediately.".to_owned()
        }
        ConcernKind::Storage => {
            "Maintenance and inspect workflows are ready. Destructive actions still confirm before running.".to_owned()
        }
    }
}

fn success_message(action: &Action) -> String {
    match action {
        Action::Query(_) => {
            "Query finished. Use 1/2/3 to switch views and y to copy the current tab.".to_owned()
        }
        Action::RecordPut(_) => {
            "Write finished. Press a to add another file to the same collection or y to copy."
                .to_owned()
        }
        Action::RecordDelete(_) => {
            "Delete finished. The result stays open so you can review or copy it.".to_owned()
        }
        Action::CollectionCreate(_) => {
            "Collection created. The result stays open; press Esc to return to the form.".to_owned()
        }
        _ => "Action finished. The result stays open so you can copy or keep browsing.".to_owned(),
    }
}

fn workflow_uses_collection(workflow: WorkflowKind) -> bool {
    !matches!(
        workflow,
        WorkflowKind::Status | WorkflowKind::ConfigShow | WorkflowKind::CollectionCreate
    )
}

fn concern_index(kind: ConcernKind) -> usize {
    match kind {
        ConcernKind::Data => 1,
        ConcernKind::Collections => 2,
        ConcernKind::Runtime => 3,
        ConcernKind::Storage => 4,
    }
}

fn move_index(current: usize, len: usize, delta: isize) -> usize {
    if len == 0 {
        return 0;
    }
    if delta.is_negative() {
        current.saturating_sub(delta.unsigned_abs())
    } else {
        (current + delta as usize).min(len - 1)
    }
}

fn panel_block(title: &str) -> Block<'static> {
    Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(CAT_SURFACE2))
        .style(Style::default().bg(CAT_MANTLE).fg(CAT_TEXT))
        .title(Span::styled(
            format!(" {title} "),
            Style::default().fg(CAT_BLUE).add_modifier(Modifier::BOLD),
        ))
}

fn error_block(title: &str) -> Block<'static> {
    Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(CAT_RED))
        .style(Style::default().bg(CAT_MANTLE).fg(CAT_TEXT))
        .title(Span::styled(
            format!(" {title} "),
            Style::default().fg(CAT_RED).add_modifier(Modifier::BOLD),
        ))
}

fn section_title_style(color: Color) -> Style {
    Style::default().fg(color).add_modifier(Modifier::BOLD)
}

fn copy_to_clipboard(text: &str) -> anyhow::Result<()> {
    let candidates: &[(&str, &[&str])] = &[
        ("pbcopy", &[]),
        ("wl-copy", &[]),
        ("xclip", &["-selection", "clipboard"]),
        ("xsel", &["--clipboard", "--input"]),
    ];
    let mut errors = Vec::new();
    for (program, args) in candidates {
        match write_to_clipboard_command(program, args, text) {
            Ok(()) => return Ok(()),
            Err(error) => errors.push(format!("{program}: {error}")),
        }
    }
    bail!(
        "no supported clipboard tool was available ({})",
        errors.join("; ")
    )
}

fn write_to_clipboard_command(program: &str, args: &[&str], text: &str) -> anyhow::Result<()> {
    let mut child = Command::new(program)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .with_context(|| format!("failed to start {program}"))?;
    if let Some(stdin) = child.stdin.as_mut() {
        stdin
            .write_all(text.as_bytes())
            .with_context(|| format!("failed to write data to {program}"))?;
    }
    let status = child
        .wait()
        .with_context(|| format!("failed to wait for {program}"))?;
    if status.success() {
        Ok(())
    } else {
        bail!("{program} exited with status {status}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_session() -> SessionContext {
        SessionContext {
            collections: vec![
                picker_choice(
                    "colors".to_owned(),
                    "colors",
                    "primary palette",
                    &["default"],
                ),
                picker_choice(
                    "shapes".to_owned(),
                    "shapes",
                    "geometry set",
                    &["secondary"],
                ),
            ],
            last_collection: Some("colors".to_owned()),
            warning: None,
        }
    }

    fn empty_args() -> InteractiveArgs {
        InteractiveArgs {
            workflow: None,
            create: false,
            collection: None,
            name: None,
            dimensions: None,
            metric: None,
            input: None,
            id: None,
            top_k: None,
            vector: None,
            filters: Vec::new(),
            where_clauses: Vec::new(),
            predicate_json: None,
            explain: None,
            segment_id: None,
        }
    }

    #[test]
    fn dashboard_filter_prefers_matching_workflow_labels() {
        let app = InteractiveApp {
            screen: Screen::Dashboard,
            home: HomeState {
                search: String::new(),
                selected: 0,
            },
            dashboard: DashboardState {
                concern: ConcernKind::Data,
                search: "query".to_owned(),
                selected: 0,
            },
            concerns: concern_definitions(),
            definitions: workflow_definitions(),
            form: None,
            confirm: None,
            picker: None,
            running: None,
            result: None,
            session: test_session(),
            output_mode: OutputMode::Human,
            cwd: PathBuf::from("."),
            status_message: String::new(),
            should_exit: false,
            exit_after_running: false,
        };

        let filtered = app.filtered_workflows();
        assert_eq!(filtered[0], WorkflowKind::Query);
    }

    #[test]
    fn query_form_parses_multiple_filters_and_predicates() {
        let args = InteractiveArgs {
            collection: Some("colors".to_owned()),
            top_k: Some(3),
            vector: Some(crate::action::QueryVector(vec![1.0, 0.0])),
            explain: Some(ExplainArg::Profile),
            ..empty_args()
        };
        let mut form =
            FormState::from_workflow(WorkflowKind::Query, &args, Path::new("."), &test_session())
                .expect("query form should build");
        form.fields
            .iter_mut()
            .find(|field| field.key == "filters")
            .expect("filters field")
            .value = "kind=article;enabled=json:true".to_owned();
        form.fields
            .iter_mut()
            .find(|field| field.key == "where")
            .expect("where field")
            .value = "kind:eq:keep;version:gte:json:7".to_owned();

        let action = form
            .build_action(Path::new("."))
            .expect("action should build");
        assert!(matches!(action, Action::Query(_)), "expected query action");
        if let Action::Query(action) = action {
            assert_eq!(action.filters.len(), 2);
            assert_eq!(action.where_clauses.len(), 2);
        }
    }

    #[test]
    fn destructive_actions_require_confirmation_prompt() {
        let action = Action::RecordDelete(RecordDeleteAction {
            collection: "colors".to_owned(),
            id: "alpha".to_owned(),
        });
        let prompt = confirmation_prompt(&action).expect("delete prompt should exist");
        assert!(prompt.contains("Delete record 'alpha'"));
    }

    #[tokio::test]
    async fn dashboard_escape_does_not_exit_the_session() {
        let mut app = InteractiveApp {
            screen: Screen::Dashboard,
            home: HomeState {
                search: String::new(),
                selected: 0,
            },
            dashboard: DashboardState {
                concern: ConcernKind::Data,
                search: String::new(),
                selected: 0,
            },
            concerns: concern_definitions(),
            definitions: workflow_definitions(),
            form: None,
            confirm: None,
            picker: None,
            running: None,
            result: None,
            session: test_session(),
            output_mode: OutputMode::Human,
            cwd: PathBuf::from("."),
            status_message: String::new(),
            should_exit: false,
            exit_after_running: false,
        };

        let should_break = app
            .handle_dashboard_key(
                KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE),
                unbounded_channel().0,
                &logpose_config::LogPoseConfig::default(),
            )
            .await
            .expect("dashboard key should succeed");

        assert!(!should_break, "escape should stay inside the session");
        assert!(!app.should_exit, "escape should not mark the app for exit");
    }

    #[test]
    fn result_back_returns_to_the_form_when_one_exists() {
        let mut app = InteractiveApp {
            screen: Screen::Result,
            home: HomeState {
                search: String::new(),
                selected: 0,
            },
            dashboard: DashboardState {
                concern: ConcernKind::Data,
                search: String::new(),
                selected: 0,
            },
            concerns: concern_definitions(),
            definitions: workflow_definitions(),
            form: Some(
                FormState::from_workflow(
                    WorkflowKind::RecordPut,
                    &InteractiveArgs {
                        workflow: None,
                        create: false,
                        collection: Some("colors".to_owned()),
                        name: None,
                        dimensions: None,
                        metric: None,
                        input: None,
                        id: None,
                        top_k: None,
                        vector: None,
                        filters: Vec::new(),
                        where_clauses: Vec::new(),
                        predicate_json: None,
                        explain: None,
                        segment_id: None,
                    },
                    Path::new("."),
                    &test_session(),
                )
                .expect("record put form should build"),
            ),
            confirm: None,
            picker: None,
            running: None,
            result: Some(ResultState {
                action: Action::Status,
                output: ActionOutput::Status(logpose_types::NodeRuntimeStatus {
                    metadata: logpose_types::NodeMetadata {
                        product: "LogPose".to_owned(),
                        node_name: "local".to_owned(),
                        version: "test".to_owned(),
                        git_sha: "test".to_owned(),
                        profile: "debug".to_owned(),
                    },
                    role: logpose_types::NodeRole::Combined,
                    rest_endpoint: "http://127.0.0.1:0".to_owned(),
                    grpc_endpoint: "http://127.0.0.1:0".to_owned(),
                    storage_engine: "local".to_owned(),
                    control_plane_ready: true,
                    data_plane_ready: true,
                    collection_count: 0,
                    collections: Vec::new(),
                    maintenance: logpose_types::MaintenanceBacklog::default(),
                }),
                tab: ResultTab::Summary,
            }),
            session: test_session(),
            output_mode: OutputMode::Human,
            cwd: PathBuf::from("."),
            status_message: String::new(),
            should_exit: false,
            exit_after_running: false,
        };

        let should_break = app.handle_result_key(KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE));

        assert!(!should_break, "back should stay inside the session");
        assert_eq!(app.screen, Screen::Form);
    }

    #[test]
    fn record_put_follow_up_keeps_collection_and_clears_input() {
        let mut form = FormState::from_workflow(
            WorkflowKind::RecordPut,
            &InteractiveArgs {
                collection: Some("colors".to_owned()),
                input: Some(PathBuf::from("records.jsonl")),
                ..empty_args()
            },
            Path::new("."),
            &test_session(),
        )
        .expect("record put form should build");
        form.fields
            .iter_mut()
            .find(|field| field.key == "input")
            .expect("input field")
            .value = "records.jsonl".to_owned();
        let mut app = InteractiveApp {
            screen: Screen::Result,
            home: HomeState {
                search: String::new(),
                selected: 0,
            },
            dashboard: DashboardState {
                concern: ConcernKind::Data,
                search: String::new(),
                selected: 0,
            },
            concerns: concern_definitions(),
            definitions: workflow_definitions(),
            form: Some(form),
            confirm: None,
            picker: None,
            running: None,
            result: Some(ResultState {
                action: Action::RecordPut(RecordPutAction {
                    collection: "colors".to_owned(),
                    input: PathBuf::from("records.jsonl"),
                }),
                output: ActionOutput::RecordsWritten(logpose_types::CommitAck {
                    applied_ops: 1,
                    last_seq_no: 7,
                }),
                tab: ResultTab::Summary,
            }),
            session: test_session(),
            output_mode: OutputMode::Human,
            cwd: PathBuf::from("."),
            status_message: String::new(),
            should_exit: false,
            exit_after_running: false,
        };

        app.prepare_follow_up_form();
        let form = app.form.as_ref().expect("form should still exist");
        let collection = form
            .fields
            .iter()
            .find(|field| field.key == "collection")
            .expect("collection field");
        let input = form
            .fields
            .iter()
            .find(|field| field.key == "input")
            .expect("input field");
        assert_eq!(collection.value, "colors");
        assert!(
            input.value.is_empty(),
            "input should clear for the next add"
        );
    }
}
