// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

/// `Cfg` is one of the two base lattices we work with (the other is the
/// user-provided so-called `Obj` object-value lattice, not defined in this
/// crate).
///
/// Cfg represents the state of the group of peers _doing_ the lattice
/// agreement. Abstractly it's a 2P-SET that stores the set of peers who have
/// been added and the set who have been removed; the set of "current members"
/// is just the adds minus the removes.
///
/// This lattice is parameterized by a user-provided notion of a Peer. This is
/// anything Ord+Clone but probably ought to be something small, like an integer
/// or UUID or string. Something that identifies a peer, and something you don't
/// mind transmitting sets of, serialized, in messages.
use pergola::{ArcOrdSetWithUnion, LatticeElt, Tuple2};
use im::OrdSet as ArcOrdSet;

pub type CfgLD<Peer> = Tuple2<ArcOrdSetWithUnion<Peer>, ArcOrdSetWithUnion<Peer>>;
pub type CfgLE<Peer> = LatticeElt<CfgLD<Peer>>;

// Helper methods on the Cfg lattice elements.
pub trait CfgLEExt<Peer: Ord + Clone> {
    fn added_peers(&self) -> &ArcOrdSet<Peer>;
    fn added_peers_mut(&mut self) -> &mut ArcOrdSet<Peer>;
    fn removed_peers(&self) -> &ArcOrdSet<Peer>;
    fn removed_peers_mut(&mut self) -> &mut ArcOrdSet<Peer>;
    fn members(&self) -> ArcOrdSet<Peer>;
}

impl<Peer: Ord + Clone> CfgLEExt<Peer> for CfgLE<Peer> {
    fn added_peers(&self) -> &ArcOrdSet<Peer> {
        &self.value.0
    }
    fn added_peers_mut(&mut self) -> &mut ArcOrdSet<Peer> {
        &mut self.value.0
    }
    fn removed_peers(&self) -> &ArcOrdSet<Peer> {
        &self.value.1
    }
    fn removed_peers_mut(&mut self) -> &mut ArcOrdSet<Peer> {
        &mut self.value.1
    }
    fn members(&self) -> ArcOrdSet<Peer> {
        self.added_peers().clone()
            .difference(self.removed_peers().clone())
    }
}
