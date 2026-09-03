use std::fmt::{Display, Formatter};
use std::io;
use std::path::{Path, PathBuf};

use chrono::Local;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConversationFileExport {
    path: PathBuf,
    overwrote_existing: bool,
}

impl ConversationFileExport {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn overwrote_existing(&self) -> bool {
        self.overwrote_existing
    }
}

#[derive(Debug)]
pub struct ConversationFileExportError {
    path: PathBuf,
    source: io::Error,
}

impl ConversationFileExportError {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn user_message(&self) -> String {
        unimplemented!("TODO: Remove");
    }
}

impl Display for ConversationFileExportError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        unimplemented!("TODO: Remove");
    }
}

impl std::error::Error for ConversationFileExportError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

pub fn export_conversation_markdown(
    current_directory: Option<&str>,
    filename_arg: Option<&str>,
    conversation_title: Option<&str>,
    markdown: &str,
) -> Result<ConversationFileExport, ConversationFileExportError> {
    unimplemented!("TODO: Remove");
}

fn conversation_export_filename_at(
    filename_arg: Option<&str>,
    conversation_title: Option<&str>,
    timestamp: &str,
) -> String {
    unimplemented!("TODO: Remove");
}
