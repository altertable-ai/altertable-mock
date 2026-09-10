use std::sync::{Arc, Mutex};

use anyhow::anyhow;
use bytes::Bytes;
use tonic::Status;

pub type TransactionId = Bytes;

pub const TRANSACTION_ALREADY_ACTIVE_MESSAGE: &str =
    "a transaction is already active; commit or rollback it first";

#[derive(Debug)]
pub struct TransactionAlreadyActive;

impl std::fmt::Display for TransactionAlreadyActive {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(TRANSACTION_ALREADY_ACTIVE_MESSAGE)
    }
}

impl std::error::Error for TransactionAlreadyActive {}

#[derive(Debug)]
pub struct TransactionNotFound;

impl std::fmt::Display for TransactionNotFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("transaction not found")
    }
}

impl std::error::Error for TransactionNotFound {}

pub enum EndAction {
    Commit,
    Rollback,
}

struct OpenTransaction {
    id: TransactionId,
}

#[derive(Clone, Default)]
pub struct ActiveTransaction {
    inner: Arc<Mutex<Option<OpenTransaction>>>,
}

impl ActiveTransaction {
    pub fn insert(&self, id: TransactionId) -> anyhow::Result<()> {
        let mut guard = self.lock()?;
        if guard.is_some() {
            return Err(anyhow!(TransactionAlreadyActive));
        }
        *guard = Some(OpenTransaction { id });
        Ok(())
    }

    pub fn require_absent(&self) -> anyhow::Result<()> {
        if self.lock()?.is_some() {
            return Err(anyhow!(TransactionAlreadyActive));
        }
        Ok(())
    }

    pub fn require(&self, id: &TransactionId) -> anyhow::Result<()> {
        match self.lock()?.as_ref() {
            Some(tx) if tx.id == *id => Ok(()),
            _ => Err(anyhow!(TransactionNotFound)),
        }
    }

    pub fn contains(&self, id: &TransactionId) -> anyhow::Result<bool> {
        Ok(self.lock()?.as_ref().is_some_and(|tx| tx.id == *id))
    }

    pub fn remove(&self, id: &TransactionId) -> anyhow::Result<bool> {
        let mut guard = self.lock()?;
        match guard.as_ref() {
            Some(tx) if tx.id == *id => {
                *guard = None;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn lock(&self) -> anyhow::Result<std::sync::MutexGuard<'_, Option<OpenTransaction>>> {
        self.inner
            .lock()
            .map_err(|e| anyhow!("Failed to lock active transaction: {e}"))
    }
}

pub fn to_status(err: &anyhow::Error, context: &str) -> Status {
    if err.is::<TransactionAlreadyActive>() {
        return Status::failed_precondition(TRANSACTION_ALREADY_ACTIVE_MESSAGE);
    }
    if err.is::<TransactionNotFound>() {
        return Status::not_found("transaction not found");
    }
    Status::internal(format!("{context}: {err}"))
}
