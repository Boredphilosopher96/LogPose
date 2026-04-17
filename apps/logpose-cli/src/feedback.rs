use std::fmt;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ProgressEvent {
    Start(String),
    Update(String),
    FinishSuccess(String),
    FinishInfo(String),
    Clear,
    Info(String),
    Warn(String),
    Error(String),
}

pub trait Reporter: Clone + Send + Sync + 'static {
    fn emit(&self, event: ProgressEvent);
}

pub struct ProgressHandle<R: Reporter> {
    reporter: R,
    finished: bool,
}

impl<R: Reporter> ProgressHandle<R> {
    pub fn start(reporter: R, message: impl Into<String>) -> Self {
        reporter.emit(ProgressEvent::Start(message.into()));
        Self {
            reporter,
            finished: false,
        }
    }

    pub fn set_message(&self, message: impl Into<String>) {
        self.reporter.emit(ProgressEvent::Update(message.into()));
    }

    pub fn finish_success(mut self, message: impl Into<String>) {
        self.finished = true;
        self.reporter
            .emit(ProgressEvent::FinishSuccess(message.into()));
    }

    pub fn finish_info(mut self, message: impl Into<String>) {
        self.finished = true;
        self.reporter
            .emit(ProgressEvent::FinishInfo(message.into()));
    }
}

impl<R: Reporter> Drop for ProgressHandle<R> {
    fn drop(&mut self) {
        if !self.finished {
            self.reporter.emit(ProgressEvent::Clear);
        }
    }
}

impl fmt::Display for ProgressEvent {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProgressEvent::Start(message)
            | ProgressEvent::Update(message)
            | ProgressEvent::FinishSuccess(message)
            | ProgressEvent::FinishInfo(message)
            | ProgressEvent::Info(message)
            | ProgressEvent::Warn(message)
            | ProgressEvent::Error(message) => formatter.write_str(message),
            ProgressEvent::Clear => formatter.write_str("clear"),
        }
    }
}
