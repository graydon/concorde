// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

use crate::{Opinion, StateLE};
use pergola::{DefTraits, LatticeDef};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::Hash;

/// Messages are either unidirectional point-to-point requests
/// or responses (matched by sequence number) or broadcast
/// commit messages sent to all peers.
#[serde(bound = "")]
#[derive(Debug, PartialEq, Eq, PartialOrd, Clone, Hash, Serialize, Deserialize)]
pub enum Message<ObjLD: LatticeDef, Peer: DefTraits> {
    Request {
        seq: u64,
        from: Peer,
        to: Peer,
        opinion: Opinion<ObjLD, Peer>,
    },
    Response {
        seq: u64,
        from: Peer,
        to: Peer,
        opinion: Opinion<ObjLD, Peer>,
    },
    Commit {
        from: Peer,
        state: StateLE<ObjLD, Peer>,
    },
}

// Conditionally implement Ord for Messages over totally-ordered lattices.
impl<ObjLD: LatticeDef, Peer: DefTraits> std::cmp::Ord for Message<ObjLD, Peer>
where
    ObjLD::T: Ord,
{
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            (Message::Request { .. }, Message::Response { .. }) => Ordering::Less,
            (Message::Request { .. }, Message::Commit { .. }) => Ordering::Less,

            (Message::Response { .. }, Message::Request { .. }) => Ordering::Greater,
            (Message::Response { .. }, Message::Commit { .. }) => Ordering::Less,

            (Message::Commit { .. }, Message::Request { .. }) => Ordering::Greater,
            (Message::Commit { .. }, Message::Response { .. }) => Ordering::Greater,

            (
                Message::Request {
                    seq: seq1,
                    from: from1,
                    to: to1,
                    opinion: opinion1,
                },
                Message::Request {
                    seq: seq2,
                    from: from2,
                    to: to2,
                    opinion: opinion2,
                },
            ) => {
                let tup1 = (seq1, from1, to1, opinion1);
                let tup2 = (seq2, from2, to2, opinion2);
                tup1.cmp(&tup2)
            }

            (
                Message::Response {
                    seq: seq1,
                    from: from1,
                    to: to1,
                    opinion: opinion1,
                },
                Message::Response {
                    seq: seq2,
                    from: from2,
                    to: to2,
                    opinion: opinion2,
                },
            ) => {
                let tup1 = (seq1, from1, to1, opinion1);
                let tup2 = (seq2, from2, to2, opinion2);
                tup1.cmp(&tup2)
            }
            (
                Message::Commit {
                    from: from1,
                    state: state1,
                },
                Message::Commit {
                    from: from2,
                    state: state2,
                },
            ) => {
                let tup1 = (from1, state1);
                let tup2 = (from2, state2);
                tup1.cmp(&tup2)
            }
        }
    }
}
