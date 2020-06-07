// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

use crate::{CfgLE, StateLE, StateLEExt};
use im::OrdSet as ArcOrdSet;
use pergola::{DefTraits, LatticeDef, LatticeElt};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::Hash;

/// `Participant`s store, exchange and update `Opinion`s about the values in the
/// Cfg and Obj lattices as they attempt to come to an agreement.
///
/// The term `Opinion` does not appear in the paper, but I've introduced it
/// (along with the field names in it) in an attempt to clarify the
/// presentation and bundle-together the 3 variables that are used as a group
/// in both local state and message bodies.
#[serde(bound = "")]
#[derive(Debug, PartialEq, Eq, PartialOrd, Clone, Hash, Default, Serialize, Deserialize)]
pub struct Opinion<ObjLD: LatticeDef, Peer: DefTraits> {
    /// called vₚ in the paper
    pub estimated_commit: StateLE<ObjLD, Peer>,
    /// called Tₚ in the paper
    pub proposed_configs: ArcOrdSet<CfgLE<Peer>>,
    /// called objₚ in the paper
    pub candidate_object: LatticeElt<ObjLD>,
}

impl<ObjLD: LatticeDef, Peer: DefTraits> Opinion<ObjLD, Peer> {
    pub fn same_estimated_commit_config(&self, other: &Self) -> bool {
        self.estimated_commit.config() == other.estimated_commit.config()
    }
    pub fn same_estimated_and_proposed_configs(&self, other: &Self) -> bool {
        self.same_estimated_commit_config(other) && self.proposed_configs == other.proposed_configs
    }
}

// Conditionally implement Ord for Messages over ordered lattices.
impl<ObjLD: LatticeDef, Peer: DefTraits> std::cmp::Ord for Opinion<ObjLD, Peer>
where
    ObjLD::T: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        let tup1 = (
            &self.estimated_commit,
            &self.proposed_configs,
            &self.candidate_object,
        );
        let tup2 = (
            &other.estimated_commit,
            &other.proposed_configs,
            &other.candidate_object,
        );
        tup1.cmp(&tup2)
    }
}
