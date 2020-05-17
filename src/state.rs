// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

use crate::CfgLE;
use pergola::{Tuple2,LatticeElt,LatticeDef};
use std::fmt::Debug;
use std::hash::Hash;

/// The State lattice combines the Cfg lattice with some
/// user-provided Obj lattice.
pub type StateLD<ObjLD,Peer> = Tuple2<LatticeElt<ObjLD>, CfgLE<Peer>>;
pub type StateLE<ObjLD,Peer> = LatticeElt<StateLD<ObjLD, Peer>>;

/// Helper methods on the State lattice elements.
pub trait StateLEExt<ObjLD:LatticeDef, Peer:Ord+Clone+Hash+Debug>
where ObjLD::T : Clone+Debug
{
    fn object(&self) -> &LatticeElt<ObjLD>;
    fn config(&self) -> &CfgLE<Peer>;
}

impl<ObjLD: LatticeDef,
     Peer: Ord+Clone+Debug+Hash>
    StateLEExt<ObjLD, Peer>
    for StateLE<ObjLD, Peer>
where ObjLD::T : Clone+Debug+Default
{
    fn object(&self) -> &LatticeElt<ObjLD> {
        &self.value.0
    }
    fn config(&self) -> &CfgLE<Peer> {
        &self.value.1
    }
}
