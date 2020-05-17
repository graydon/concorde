// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

use crate::{Opinion, StateLE};
use pergola::LatticeDef;
use std::cmp::Ordering;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

/// Messages are either unidirectional point-to-point requests
/// or responses (matched by sequence number) or broadcast
/// commit messages sent to all peers.
#[derive(Debug)]
pub enum Message<ObjLD: LatticeDef, Peer: Ord + Clone + Debug + Hash>
where
    ObjLD::T: Clone + Debug + Default,
{
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

// Manually implement Clone since #[derive(Clone)] fails here,
// see bug https://github.com/rust-lang/rust/issues/26925
impl<ObjLD: LatticeDef, Peer: Ord + Clone + Debug + Hash> std::clone::Clone for Message<ObjLD, Peer>
where
    ObjLD::T: Clone + Debug + Default,
{
    fn clone(&self) -> Self {
        match self {
            Message::Request {
                seq,
                from,
                to,
                opinion,
            } => Message::Request {
                seq: *seq,
                from: from.clone(),
                to: to.clone(),
                opinion: opinion.clone(),
            },
            Message::Response {
                seq,
                from,
                to,
                opinion,
            } => Message::Response {
                seq: *seq,
                from: from.clone(),
                to: to.clone(),
                opinion: opinion.clone(),
            },
            Message::Commit { from, state } => Message::Commit {
                from: from.clone(),
                state: state.clone(),
            },
        }
    }
}

// Manually implement PartialEq since #[derive(PartialEq)] fails here,
// see bug https://github.com/rust-lang/rust/issues/26925
impl<ObjLD: LatticeDef, Peer: Ord + Clone + Debug + Hash> std::cmp::PartialEq
    for Message<ObjLD, Peer>
where
    ObjLD::T: Clone + Debug + Default + Eq,
{
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
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
                tup1.eq(&tup2)
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
                tup1.eq(&tup2)
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
                tup1.eq(&tup2)
            }
            _ => false,
        }
    }
}

// Conditionally implement Eq for Messages over lattices with equality.
impl<ObjLD: LatticeDef, Peer: Ord + Clone + Debug + Hash> std::cmp::Eq for Message<ObjLD, Peer> where
    ObjLD::T: Clone + Debug + Default + Eq
{
}

// Manually implement PartialOrd since #[derive(PartialOrd)] fails here,
// see bug https://github.com/rust-lang/rust/issues/26925
impl<ObjLD: LatticeDef, Peer: Ord + Clone + Debug + Hash> std::cmp::PartialOrd
    for Message<ObjLD, Peer>
where
    ObjLD::T: Clone + Debug + Default + Ord,
{
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Message::Request { .. }, Message::Response { .. }) => Some(Ordering::Less),
            (Message::Request { .. }, Message::Commit { .. }) => Some(Ordering::Less),

            (Message::Response { .. }, Message::Request { .. }) => Some(Ordering::Greater),
            (Message::Response { .. }, Message::Commit { .. }) => Some(Ordering::Less),

            (Message::Commit { .. }, Message::Request { .. }) => Some(Ordering::Greater),
            (Message::Commit { .. }, Message::Response { .. }) => Some(Ordering::Greater),

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
                tup1.partial_cmp(&tup2)
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
                tup1.partial_cmp(&tup2)
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
                tup1.partial_cmp(&tup2)
            }
        }
    }
}

// Conditionally implement Ord for Messages over totally-ordered lattices.
impl<ObjLD: LatticeDef, Peer: Ord + Clone + Debug + Hash> std::cmp::Ord for Message<ObjLD, Peer>
where
    ObjLD::T: Clone + Debug + Default + Ord,
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

impl<ObjLD: LatticeDef, Peer: Ord + Clone + Debug + Hash> Hash for Message<ObjLD, Peer>
where
    ObjLD::T: Clone + Debug + Default + Hash,
{
    fn hash<H: Hasher>(&self, hstate: &mut H) {
        match self {
            Message::Request {
                seq,
                from,
                to,
                opinion,
            } => {
                (1u8).hash(hstate);
                seq.hash(hstate);
                from.hash(hstate);
                to.hash(hstate);
                opinion.hash(hstate);
            }
            Message::Response {
                seq,
                from,
                to,
                opinion,
            } => {
                (2u8).hash(hstate);
                seq.hash(hstate);
                from.hash(hstate);
                to.hash(hstate);
                opinion.hash(hstate);
            }
            Message::Commit { from, state } => {
                (3u8).hash(hstate);
                from.hash(hstate);
                state.hash(hstate);
            }
        }
    }
}
