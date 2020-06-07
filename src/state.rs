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
    fn object(&self) -> &ObjLD::T;
    fn config(&self) -> &<CfgLD<Peer> as LatticeDef>::T;
}

impl<ObjLD: LatticeDef, Peer: DefTraits> StateLEExt<ObjLD, Peer> for StateLE<ObjLD, Peer> {
    fn object(&self) -> &ObjLD::T {
        &self.value.0
    }
    fn config(&self) -> &<CfgLD<Peer> as LatticeDef>::T {
        &self.value.1
    }
}
