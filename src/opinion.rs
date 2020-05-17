// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

use crate::{CfgLE, StateLE, StateLEExt};
use pergola::{LatticeDef, LatticeElt};
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

/// `Participant`s store, exchange and update `Opinion`s about the values in the
/// Cfg and Obj lattices as they attempt to come to an agreement.
///
/// The term `Opinion` does not appear in the paper, but I've introduced it
/// (along with the field names in it) in an attempt to clarify the
/// presentation and bundle-together the 3 variables that are used as a group
/// in both local state and message bodies.
#[derive(Debug)]
pub struct Opinion<ObjLD: LatticeDef, Peer: Ord + Clone + Debug + Hash>
where
    ObjLD::T: Clone + Debug + Default,
{
    /// called vₚ in the paper
    pub estimated_commit: StateLE<ObjLD, Peer>,
    /// called Tₚ in the paper
    pub proposed_configs: BTreeSet<CfgLE<Peer>>,
    /// called objₚ in the paper
    pub candidate_object: LatticeElt<ObjLD>,
}

impl<ObjLD: LatticeDef, Peer: Ord + Clone + Debug + Hash> Opinion<ObjLD, Peer>
where
    ObjLD::T: Clone + Debug + Default,
{
    pub fn same_estimated_commit_config(&self, other: &Self) -> bool {
        self.estimated_commit.config() == other.estimated_commit.config()
    }
    pub fn same_estimated_and_proposed_configs(&self, other: &Self) -> bool {
        self.same_estimated_commit_config(other) && self.proposed_configs == other.proposed_configs
    }
}

// Manually implement Default since #[derive(Default)] fails here,
// see bug https://github.com/rust-lang/rust/issues/26925
impl<ObjLD: LatticeDef, Peer: Ord + Clone + Debug + Hash> std::default::Default
    for Opinion<ObjLD, Peer>
where
    ObjLD::T: Clone + Debug + Default,
{
    fn default() -> Self {
        Opinion {
            estimated_commit: StateLE::<ObjLD, Peer>::default(),
            proposed_configs: BTreeSet::default(),
            candidate_object: LatticeElt::<ObjLD>::default(),
        }
    }
}

// Manually implement Clone since #[derive(Clone)] fails here,
// see bug https://github.com/rust-lang/rust/issues/26925
impl<ObjLD: LatticeDef, Peer: Ord + Clone + Debug + Hash> std::clone::Clone for Opinion<ObjLD, Peer>
where
    ObjLD::T: Clone + Debug + Default,
{
    fn clone(&self) -> Self {
        Opinion {
            estimated_commit: self.estimated_commit.clone(),
            proposed_configs: self.proposed_configs.clone(),
            candidate_object: self.candidate_object.clone(),
        }
    }
}

// Manually implement PartialEq since #[derive(PartialEq)] fails here,
// see bug https://github.com/rust-lang/rust/issues/26925
impl<ObjLD: LatticeDef, Peer: Ord + Clone + Debug + Hash> std::cmp::PartialEq
    for Opinion<ObjLD, Peer>
where
    ObjLD::T: Clone + Debug + Default + Eq,
{
    fn eq(&self, other: &Self) -> bool {
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
        tup1.eq(&tup2)
    }
}

// Manually implement PartialOrd since #[derive(PartialOrd)] fails here,
// see bug https://github.com/rust-lang/rust/issues/26925
impl<ObjLD: LatticeDef, Peer: Ord + Clone + Debug + Hash> std::cmp::PartialOrd
    for Opinion<ObjLD, Peer>
where
    ObjLD::T: Clone + Debug + Default + Eq,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
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
        tup1.partial_cmp(&tup2)
    }
}

// Conditionally implement Eq for Messages over lattices with equality.
impl<ObjLD: LatticeDef, Peer: Ord + Clone + Debug + Hash> std::cmp::Eq for Opinion<ObjLD, Peer> where
    ObjLD::T: Clone + Debug + Default + Eq
{
}

// Conditionally implement Ord for Messages over ordered lattices.
impl<ObjLD: LatticeDef, Peer: Ord + Clone + Debug + Hash> std::cmp::Ord for Opinion<ObjLD, Peer>
where
    ObjLD::T: Clone + Debug + Default + Ord,
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

impl<ObjLD: LatticeDef, Peer: Ord + Clone + Debug + Hash> Hash for Opinion<ObjLD, Peer>
where
    ObjLD::T: Clone + Debug + Default + Hash,
{
    fn hash<H: Hasher>(&self, hstate: &mut H) {
        self.estimated_commit.hash(hstate);
        self.proposed_configs.hash(hstate);
        self.candidate_object.hash(hstate);
    }
}
