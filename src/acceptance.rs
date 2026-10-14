//! The source of accepted message commitments for content admission.
//! Indexed hosts never bypass a warming or stopped index. Store-only hosts
//! read the Board at their accepted depth, sharing one head per lookup pass.
//! Provisional evidence is separate: it may justify holding or fetching bytes,
//! but never promotion into the served store.

use crate::{
    server::AppState,
    verifier::{MessageInfo, VerifyError},
};
use std::collections::HashMap;

pub struct Acceptance<'a> {
    state: &'a AppState,
    heads: HashMap<u64, Result<u64, VerifyError>>,
}

impl<'a> Acceptance<'a> {
    pub fn new(state: &'a AppState) -> Self {
        Self {
            state,
            heads: HashMap::new(),
        }
    }

    /// The accepted block a store-only host reads the Board at, looked up
    /// once per pass per chain; `None` on a host whose index answers instead.
    pub async fn confirmed(&mut self, chain: u64) -> Result<Option<u64>, VerifyError> {
        if self.state.gateway.is_some() {
            return Ok(None);
        }
        match self.heads.get(&chain) {
            Some(result) => result.clone(),
            None => {
                let result = self.state.verifier.confirmed_block(chain).await;
                self.heads.insert(chain, result.clone());
                result
            }
        }
        .map(Some)
    }

    /// Accepted facts only. Absence is retryable; unavailability is an error.
    pub async fn accepted(
        &mut self,
        chain: u64,
        id: u64,
    ) -> Result<Option<MessageInfo>, VerifyError> {
        let Some(block) = self.confirmed(chain).await? else {
            return self.state.indexed_message(chain, id);
        };
        optional(self.state.verifier.message_at(chain, id, Some(block)).await)
    }

    /// A peer hint may use latest-chain evidence on a store-only host. Its
    /// returned bytes still pass upload admission and may be held, not served.
    pub async fn for_peer(&self, chain: u64, id: u64) -> Result<Option<MessageInfo>, VerifyError> {
        if self.state.gateway.is_some() {
            self.state.indexed_message(chain, id)
        } else {
            optional(self.state.verifier.message_at(chain, id, None).await)
        }
    }
}

fn optional(result: Result<MessageInfo, VerifyError>) -> Result<Option<MessageInfo>, VerifyError> {
    match result {
        Ok(message) => Ok(Some(message)),
        Err(VerifyError::NotAnchored(_)) => Ok(None),
        Err(error) => Err(error),
    }
}
