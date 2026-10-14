//! Query-time list exclusions leave shared records and reference associations intact.
use crate::chain::Address;
use crate::index::{model::Model, types::*};
use anyhow::Result;
use std::collections::BTreeSet;

pub(crate) struct Exclusion<'a> {
    members: Option<&'a BTreeSet<Address>>,
}
impl<'a> Exclusion<'a> {
    pub fn new(model: &'a Model, list: Option<ListKey>) -> Result<Self> {
        if let Some(list) = list {
            super::require_registry(model, list.registry)?;
        }
        Ok(Self {
            members: list.and_then(|key| model.lists.get(&key)),
        })
    }
    pub fn contains(&self, author: Address) -> bool {
        self.members
            .is_some_and(|members| members.contains(&author))
    }
}
