//! Helper traits and types to make `std::process::Command` more ergonomic

#![warn(missing_docs)]

use std::process::Command;

/// `Ok(())` when a child process successfully runs and returns exit code `0`.
///
/// All other circumstances are regarded as `Err(CommandError)`. This includes
/// when there is an error invoking a child process, if a child process is
/// terminated, or if the child process runs and returns a non-zero exit code.
pub type CommandResult = Result<(), CommandError>;

/// An error type describing the kinds of failure a child process may have
#[derive(thiserror::Error, Debug)]
#[error("Failed to run \"{command}\"")]
pub struct CommandError {
    /// The command that was attempting to run
    pub command: String,
    /// The kind of error that the command encountered
    pub source: CommandErrorKind,
}

/// Describes the particular kinds of errors that may occur while running a command
#[derive(thiserror::Error, Debug)]
pub enum CommandErrorKind {
    /// The child process was terminated, so did not exit successfully
    #[error("Child process was terminated and has no exit code")]
    Terminated,
    /// The child process completed with a non-zero exit code
    #[error("Child process completed with non-zero exit code {0}")]
    ExitError(i32),
    /// There was an error invoking the command
    #[error("An error occurred while invoking child process")]
    IoError(#[from] std::io::Error),
}

/// Adds useful extension methods to the `Command` type
pub trait CommandExt {
    /// Inherit both `stdout` and `stderr` from this process.
    ///
    /// # Example
    ///
    /// ```
    /// use std::process::Command;
    /// use fluvio_command::CommandExt;
    /// let _ = Command::new("echo")
    ///     .arg("Hello world")
    ///     .inherit()
    ///     .status();
    /// ```
    fn inherit(&mut self) -> &mut Self;
    /// Return a stringified version of the Command.
    ///
    /// ```
    /// use std::process::Command;
    /// use fluvio_command::CommandExt;
    /// let mut command = Command::new("echo");
    /// command.arg("one").arg("two three");
    /// let command_string: String = command.display();
    /// assert_eq!(command_string, "echo one two three");
    /// ```
    fn display(&mut self) -> String;
    /// Returns a result signaling the outcome of executing this command.
    ///
    /// ```
    /// use std::process::Command;
    /// use fluvio_command::{CommandExt, CommandErrorKind};
    ///
    /// let success = Command::new("true").result().unwrap();
    ///
    /// let error = Command::new("false").result().unwrap_err();
    /// assert!(matches!(error.source, CommandErrorKind::ExitError(1)));
    /// assert_eq!(error.command, "false");
    ///
    /// let error = Command::new("foobar").result().unwrap_err();
    /// assert!(matches!(error.source, CommandErrorKind::IoError(_)));
    /// assert_eq!(error.command, "foobar");
    /// ```
    fn result(&mut self) -> CommandResult;
}

impl CommandExt for Command {
    fn inherit(&mut self) -> &mut Self {
        use std::process::Stdio;
        self.stdout(Stdio::inherit()).stderr(Stdio::inherit())
    }

    fn display(&mut self) -> String {
        format!("{:?}", self).replace("\"", "")
    }

    fn result(&mut self) -> CommandResult {
        self.status()
            .map_err(|e| CommandError {
                command: self.display(),
                source: CommandErrorKind::IoError(e),
            })
            .and_then(|status| match status.code() {
                Some(0i32) => Ok(()),
                None => Err(CommandError {
                    command: self.display(),
                    source: CommandErrorKind::Terminated,
                }),
                Some(code) => Err(CommandError {
                    command: self.display(),
                    source: CommandErrorKind::ExitError(code),
                }),
            })
    }
}
