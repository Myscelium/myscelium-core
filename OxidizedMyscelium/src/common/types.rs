use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SchedulingError {
    ClientIsntFullyInitialized(String),
    CantReadStates,
    TargetDoesntExists(String),
    HandlerDoesntExist(String),
    ResponseHandlerDoesntExist(String),
    CantScheduleCommandsToItself(String),
    HostCantSendResponseToItself,
    TargetCantSendResponseToItself,
    UnsuportedAction(String),
    BufferError(String),
}

impl From<BufferError> for SchedulingError {
    fn from(e: BufferError) -> SchedulingError {
        match e {
            BufferError::UnexpectedError(e) => SchedulingError::BufferError(e),
        }
    }
}

#[derive(Debug)]
pub enum BufferError {
    UnexpectedError(String),
}
