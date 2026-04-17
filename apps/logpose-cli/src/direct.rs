use crate::feedback::{ProgressEvent, Reporter};
use anyhow::{Context, bail};
use std::{
    io::{self, IsTerminal, Write},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};

#[derive(Clone, Copy)]
enum MessageTone {
    Accent,
    Info,
    Success,
    Warn,
    Error,
}

#[derive(Clone)]
pub struct TerminalUi {
    color_enabled: bool,
    spinner_enabled: bool,
    stdin_is_terminal: bool,
    stderr_is_terminal: bool,
}

impl TerminalUi {
    pub fn detect() -> Self {
        let stdin_is_terminal = io::stdin().is_terminal();
        let stderr_is_terminal = io::stderr().is_terminal();
        let no_color = std::env::var_os("NO_COLOR").is_some();
        Self {
            color_enabled: stderr_is_terminal && !no_color,
            spinner_enabled: stderr_is_terminal,
            stdin_is_terminal,
            stderr_is_terminal,
        }
    }

    pub fn supports_fullscreen(&self) -> bool {
        self.stdin_is_terminal && self.stderr_is_terminal
    }

    pub fn error_report(&self, error: &anyhow::Error) {
        self.error(&error.to_string());
        for cause in error.chain().skip(1) {
            eprintln!("  {} {cause}", self.decorate("[cause]", MessageTone::Warn));
        }
    }

    pub fn section(&self, title: &str) {
        eprintln!(
            "\n{} {}",
            self.decorate("[step]", MessageTone::Accent),
            title
        );
    }

    pub fn info(&self, message: &str) {
        self.print(MessageTone::Info, "[info]", message);
    }

    pub fn success(&self, message: &str) {
        self.print(MessageTone::Success, "[ok]", message);
    }

    pub fn warn(&self, message: &str) {
        self.print(MessageTone::Warn, "[warn]", message);
    }

    pub fn error(&self, message: &str) {
        self.print(MessageTone::Error, "[error]", message);
    }

    pub fn print_choice(&self, number: usize, label: &str, detail: &str) {
        eprintln!(
            "  {} {}  {detail}",
            self.decorate(&format!("{number}."), MessageTone::Info),
            self.bold(label)
        );
    }

    pub fn prompt_required_string(
        &self,
        label: &str,
        default: Option<&str>,
        example: Option<&str>,
    ) -> anyhow::Result<String> {
        loop {
            match self.prompt_string(label, default, example, true)? {
                Some(value) if !value.trim().is_empty() => return Ok(value),
                Some(_) | None => self.warn("Enter a value to continue."),
            }
        }
    }

    pub fn prompt_optional_string(
        &self,
        label: &str,
        example: Option<&str>,
    ) -> anyhow::Result<Option<String>> {
        self.prompt_string(label, None, example, false)
    }

    pub fn prompt_usize(&self, label: &str, default: usize) -> anyhow::Result<usize> {
        loop {
            let value = self.prompt_required_string(label, Some(&default.to_string()), None)?;
            match value.parse::<usize>() {
                Ok(parsed) => return Ok(parsed),
                Err(error) => self.warn(&format!("invalid {label}: {error}")),
            }
        }
    }

    pub fn prompt_required_parsed<T>(
        &self,
        label: &str,
        default: Option<&str>,
        example: Option<&str>,
        mut parse: impl FnMut(&str) -> Result<T, String>,
    ) -> anyhow::Result<T> {
        loop {
            let value = self.prompt_required_string(label, default, example)?;
            match parse(&value) {
                Ok(parsed) => return Ok(parsed),
                Err(error) => self.warn(&error),
            }
        }
    }

    pub fn prompt_optional_parsed<T>(
        &self,
        label: &str,
        example: Option<&str>,
        mut parse: impl FnMut(&str) -> Result<T, String>,
    ) -> anyhow::Result<Option<T>> {
        loop {
            let Some(value) = self.prompt_optional_string(label, example)? else {
                return Ok(None);
            };
            match parse(&value) {
                Ok(parsed) => return Ok(Some(parsed)),
                Err(error) => self.warn(&error),
            }
        }
    }

    pub fn confirm(&self, label: &str, default: bool) -> anyhow::Result<bool> {
        let default_text = if default { "y" } else { "n" };
        loop {
            let answer = self.prompt_required_string(label, Some(default_text), Some("y or n"))?;
            match answer.to_ascii_lowercase().as_str() {
                "y" | "yes" => return Ok(true),
                "n" | "no" => return Ok(false),
                _ => self.warn("Enter 'y' or 'n'."),
            }
        }
    }

    fn prompt_string(
        &self,
        label: &str,
        default: Option<&str>,
        example: Option<&str>,
        required: bool,
    ) -> anyhow::Result<Option<String>> {
        let mut prompt = label.to_owned();
        if let Some(default) = default {
            prompt.push_str(&format!(" [default: {default}]"));
        } else if let Some(example) = example {
            prompt.push_str(&format!(" [example: {example}]"));
        }
        prompt.push_str(": ");

        eprint!("{prompt}");
        io::stderr().flush().context("failed to flush prompt")?;

        let mut buffer = String::new();
        let read = io::stdin()
            .read_line(&mut buffer)
            .context("failed to read interactive input")?;
        if read == 0 {
            bail!("interactive input ended before '{label}' was provided");
        }

        let trimmed = buffer.trim();
        if trimmed.is_empty() {
            if let Some(default) = default {
                return Ok(Some(default.to_owned()));
            }
            if required {
                return Ok(None);
            }
            return Ok(None);
        }

        Ok(Some(trimmed.to_owned()))
    }

    fn print(&self, tone: MessageTone, label: &str, message: &str) {
        eprintln!("{} {message}", self.decorate(label, tone));
    }

    fn decorate(&self, label: &str, tone: MessageTone) -> String {
        if !self.color_enabled {
            return label.to_owned();
        }
        let code = match tone {
            MessageTone::Accent => "1;35",
            MessageTone::Info => "1;36",
            MessageTone::Success => "1;32",
            MessageTone::Warn => "1;33",
            MessageTone::Error => "1;31",
        };
        format!("\x1b[{code}m{label}\x1b[0m")
    }

    fn bold(&self, label: &str) -> String {
        if self.color_enabled {
            format!("\x1b[1m{label}\x1b[0m")
        } else {
            label.to_owned()
        }
    }
}

struct SpinnerSession {
    stop: Arc<AtomicBool>,
    message: Arc<Mutex<String>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl SpinnerSession {
    fn new(message: &str) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        let message_state = Arc::new(Mutex::new(message.to_owned()));
        let stop_thread = Arc::clone(&stop);
        let message_thread = Arc::clone(&message_state);
        let handle = thread::spawn(move || {
            let frames = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];
            let mut index = 0usize;
            while !stop_thread.load(Ordering::Relaxed) {
                let message = message_thread
                    .lock()
                    .map(|guard| guard.clone())
                    .unwrap_or_else(|_| "Working...".to_owned());
                eprint!("\r\x1b[2K{} {}", frames[index % frames.len()], message);
                let _ = io::stderr().flush();
                index += 1;
                thread::sleep(Duration::from_millis(80));
            }
            eprint!("\r\x1b[2K");
            let _ = io::stderr().flush();
        });
        Self {
            stop,
            message: message_state,
            handle: Some(handle),
        }
    }

    fn set_message(&self, message: &str) {
        if let Ok(mut guard) = self.message.lock() {
            *guard = message.to_owned();
        }
    }

    fn stop(mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

#[derive(Clone)]
pub struct DirectReporter {
    ui: TerminalUi,
    spinner: Arc<Mutex<Option<SpinnerSession>>>,
}

impl DirectReporter {
    pub fn new(ui: &TerminalUi) -> Self {
        Self {
            ui: ui.clone(),
            spinner: Arc::new(Mutex::new(None)),
        }
    }

    fn start_spinner(&self, message: &str) {
        if !self.ui.spinner_enabled {
            self.ui.info(message);
            return;
        }
        let mut guard = self.spinner.lock().expect("spinner mutex should lock");
        if let Some(active) = guard.take() {
            active.stop();
        }
        *guard = Some(SpinnerSession::new(message));
    }

    fn update_spinner(&self, message: &str) {
        let guard = self.spinner.lock().expect("spinner mutex should lock");
        if let Some(active) = guard.as_ref() {
            active.set_message(message);
        } else {
            self.ui.info(message);
        }
    }

    fn stop_spinner(&self) {
        let mut guard = self.spinner.lock().expect("spinner mutex should lock");
        if let Some(active) = guard.take() {
            active.stop();
        }
    }
}

impl Reporter for DirectReporter {
    fn emit(&self, event: ProgressEvent) {
        match event {
            ProgressEvent::Start(message) => self.start_spinner(&message),
            ProgressEvent::Update(message) => self.update_spinner(&message),
            ProgressEvent::FinishSuccess(message) => {
                self.stop_spinner();
                self.ui.success(&message);
            }
            ProgressEvent::FinishInfo(message) => {
                self.stop_spinner();
                self.ui.info(&message);
            }
            ProgressEvent::Clear => self.stop_spinner(),
            ProgressEvent::Info(message) => self.ui.info(&message),
            ProgressEvent::Warn(message) => self.ui.warn(&message),
            ProgressEvent::Error(message) => self.ui.error(&message),
        }
    }
}
