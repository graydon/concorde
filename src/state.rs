// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

use crate::CfgLD;
use pergola::{DefTraits, LatticeDef, LatticeElt, Tuple2};

/// The State lattice combines the Cfg lattice with some
/// user-provided Obj lattice.
pub type StateLD<ObjLD, Peer> = Tuple2<ObjLD, CfgLD<Peer>>;
pub type StateLE<ObjLD, Peer> = LatticeElt<StateLD<ObjLD, Peer>>;

/// Helper methods on the State lattice elements.
pub trait StateLEExt<ObjLD: LatticeDef, Peer: DefTraits> {
    fn object(&self) -> &LatticeElt<ObjLD>;
    fn config(&self) -> &LatticeElt<CfgLD<Peer>>;
}

impl<ObjLD: LatticeDef, Peer: DefTraits> StateLEExt<ObjLD, Peer> for StateLE<ObjLD, Peer> {
    fn object(&self) -> &LatticeElt<ObjLD> {
        &self.value.0
    }
    fn config(&self) -> &LatticeElt<CfgLD<Peer>> {
        &self.value.1
    }
}
