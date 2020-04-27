// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

/*!
 * This crate is a work in progress attempt to implement a small, special-case
 * consensus-like algorithm called "reconfigurable lattice agreement", which has
 * some desirable properties:
 *
 *   - It's small, fast and simple compared with other consensus-like
 *     algorithms. Few states, message types and round-trips. The paper
 *     introducing it describes it in 20 lines of pseudocode.
 *
 *   - It supports _online reconfiguration_ without any separate phases: you can
 *     add or subtract peers while it's running and it adapts to the changed
 *     quorum on the fly.
 *
 * The price for these desirable properties is high relative to general
 * consensus, but a price you may able and willing to pay:
 *
 *   - The "object" domain of discourse -- about which you're trying to come
 *     to agreement -- has to be a (join semi-)lattice.
 *
 *   - You have to be ok with the "lattice agreement" API, which is one
 *     where you might get an "object" value that's possibly further
 *     up its lattice from the value you proposed, and might not even be
 *     a value that anyone proposed (just a join of proposals).
 *
 * Further, the representation of your quorum system (eg. a set of peers) has
 * itself to be a lattice, though in this implementation it is a fixed peer-set
 * quorum system, so you don't really get an option about that here.
 *
 * ## Reference
 *
 * Petr Kuznetsov, Thibault Rieutord, Sara Tucci-Piergiovanni.
 * Reconfigurable Lattice Agreement and Applications. [Research Report]
 * Institut Polytechnique Paris; CEA List. 2019. ffcea-02321547f
 *
 * https://hal-cea.archives-ouvertes.fr/cea-02321547
 *
 * ## Name
 *
 * Wikipedia:
 *
 * > The Aérospatiale/BAC Concorde is a British–French turbojet-powered
 * > supersonic passenger airliner
 * >
 * > ...
 * >
 * > Concorde is from the French word concorde, which has an English
 * > equivalent, concord. Both words mean agreement, harmony or union.
 */

// TODO: tests & fix bugs.
// TODO: error handling, timeouts, get rid of that unwrap().
// TODO: add a trim watermark to CfgLD so it's not ever-growing.

use pergola::*;
use std::collections::{BTreeSet};
use std::fmt::Debug;
use async_std::sync::{Sender,Receiver};
use itertools::Itertools;

/// `Cfg` is one of the two base lattices we work with (the other is the
/// user-provided so-called `Obj` object-value lattice). Cfg represents the
/// state of the group of peers _doing_ the lattice agreement. Abstractly it's a
/// 2P-SET that stores the set of peers who have been added and the set who have
/// been removed; the set of "current members" is just the adds minus the
/// removes.
///
/// This lattice is parameterized by a user-provided notion of a Peer. This is
/// anything Ord+Clone but probably ought to be something small, like an integer
/// or UUID or string. Something that identifies a peer, and something you don't
/// mind transmitting sets of, serialized, in messages.
type CfgLD<Peer> = Tuple2<BTreeSetWithUnion<Peer>,
                          BTreeSetWithUnion<Peer>>;
type CfgLE<Peer> = LatticeElt<CfgLD<Peer>>;

// Helper methods on the Cfg lattice elements.
trait CfgLEExt<Peer:Ord+Clone>
{
    fn added_peers(&self) -> &BTreeSet<Peer>;
    fn added_peers_mut(&mut self) -> &mut BTreeSet<Peer>;
    fn removed_peers(&self) -> &BTreeSet<Peer>;
    fn removed_peers_mut(&mut self) -> &mut BTreeSet<Peer>;
    fn members(&self) -> BTreeSet<Peer>;
}

impl<Peer:Ord+Clone>
    CfgLEExt<Peer>
    for CfgLE<Peer>
{
    fn added_peers(&self) -> &BTreeSet<Peer> {
        &self.value.0
    }
    fn added_peers_mut(&mut self) -> &mut BTreeSet<Peer> {
        &mut self.value.0
    }
    fn removed_peers(&self) -> &BTreeSet<Peer> {
        &self.value.1
    }
    fn removed_peers_mut(&mut self) -> &mut BTreeSet<Peer> {
        &mut self.value.1
    }
    fn members(&self) -> BTreeSet<Peer>
    {
        self.added_peers().difference(self.removed_peers()).cloned().collect()
    }
}

/// The State lattice combines the Cfg lattice above with some
/// user-provided Obj lattice.
type StateLD<ObjLD,Peer> = Tuple2<LatticeElt<ObjLD>,
                                  CfgLE<Peer>>;
type StateLE<ObjLD,Peer> = LatticeElt<StateLD<ObjLD,Peer>>;

/// Helper methods on the State lattice elements.
trait StateLEExt<ObjLD:LatticeDef,
                 Peer:Ord+Clone+Debug+Default>
where ObjLD::T : Clone+Debug
{
    fn object(&self) -> &LatticeElt<ObjLD>;
    fn config(&self) -> &CfgLE<Peer>;
}

impl<ObjLD: LatticeDef,
     Peer: Ord+Clone+Debug+Default>
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

/// `Participant`s store, exchange and update `Opinion`s about the values in the
/// Cfg and Obj lattices as they attempt to come to an agreement.
///
/// The term `Opinion` does not appear in the paper, but I've introduced it
/// (along with the field names in it) in an attempt to clarify the
/// presentation and bundle-together the 3 variables that are used as a group
/// in both local state and message bodies.
#[derive(Debug)]
pub struct Opinion<ObjLD: LatticeDef,
                   Peer: Ord+Clone+Debug>
where ObjLD::T : Clone+Debug+Default
{
    /// called vₚ in the paper
    estimated_commit: StateLE<ObjLD,Peer>,
    /// called Tₚ in the paper
    proposed_configs: BTreeSet<CfgLE<Peer>>,
    /// called objₚ in the paper
    candidate_object: LatticeElt<ObjLD>,
}

// Either Derive(Default) is not working reliably here,
// or I am not setting up its prerequisites correctly.
// Either way I can't get it to work so: manually!
impl<ObjLD: LatticeDef,
     Peer: Ord+Clone+Debug>
    std::default::Default
    for Opinion<ObjLD,Peer>
where ObjLD::T : Clone+Debug+Default
{
    fn default() -> Self
    {
        Opinion
        {
            estimated_commit: StateLE::<ObjLD,Peer>::default(),
            proposed_configs: BTreeSet::default(),
            candidate_object: LatticeElt::<ObjLD>::default()
        }
    }
}

// Either Derive(Clone) is not working reliably here,
// or I am not setting up its prerequisites correctly.
// Either way I can't get it to work so: manually!
impl<ObjLD: LatticeDef,
     Peer: Ord+Clone+Debug>
    std::clone::Clone
    for Opinion<ObjLD,Peer>
where ObjLD::T : Clone+Debug+Default
{
    fn clone(&self) -> Self
    {
        Opinion
        {
            estimated_commit: self.estimated_commit.clone(),
            proposed_configs: self.proposed_configs.clone(),
            candidate_object: self.candidate_object.clone()
        }
    }
}

/// Messages are either:
///
///   - Unidirectional point-to-point `Request`s with
///     matching `Response`s (related by sequence number)
///
///  or
///
///   - Broadcast `Commit` messages sent to all peers.
///
#[derive(Clone,Debug)]
pub enum Message<ObjLD: LatticeDef,
                 Peer: Ord+Clone+Debug+Default>
where ObjLD::T : Clone+Debug+Default
{
    Request{seq: u64, from: Peer, to: Peer, opinion: Opinion<ObjLD,Peer>},
    Response{seq: u64, from: Peer, to: Peer, opinion: Opinion<ObjLD,Peer>},
    Commit(StateLE<ObjLD,Peer>)
}

/// `Participant`s represent parties participating in lattice agreement. They
/// support a single public asynchronous operation `propose` that returns
/// a `Future` that resolves once an agreeable state has been found. Note that
/// unlike in a full consensus system, the response from `propose` is _not_
/// guaranteed to be equal at all `Participant`s, nor is it guaranteed to even
/// be a value any `Participant` proposed.
///
/// (The exact guarantees on the proposal, its response, and its relationships
///  to other proposals and responses on other peers are somewhat subtle, see
///  the paper.)
///
/// Participants must be instantiated with a sending and receiving channel, each
/// of which carries typed `Message`s. Responsibility for routing
/// `Message::Request`s and `Message::Response`s to their destination `Peer`s is
/// left to the client of this library, as is responsibility for distributing
/// broadcast `Message::Commit`s.
///
/// All such communication is assumed to be (eventually) reliable, though no
/// timing assumptions are made. Lost messages, or a sufficient number of
/// quorum-peer failures, may cause `Participant`s to get stuck.
pub struct Participant<ObjLD: LatticeDef,
                       Peer: Ord+Clone+Debug+Default>
where ObjLD::T : Clone+Debug+Default
{
    sequence: u64,
    opinion: Opinion<ObjLD,Peer>,

    // Peers who have responded with Response(seq) to Request(seq)
    // requests. Reset when sequence is advanced.
    sequence_responses: BTreeSet<Peer>,

    id: Peer,
    sender: Sender<Message<ObjLD,Peer>>,
    receiver: Receiver<Message<ObjLD,Peer>>
}


impl<ObjLD:LatticeDef,
     Peer:Ord+Clone+Debug+Default>
    Participant<ObjLD, Peer>
where ObjLD::T : Clone+Debug+Default
{
    pub fn new(id: Peer,
               s: Sender<Message<ObjLD,Peer>>,
               r: Receiver<Message<ObjLD,Peer>>) -> Self
    {
        Participant
        {
            sequence: 0,
            opinion: Opinion::default(),
            sequence_responses: BTreeSet::new(),
            id: id,
            sender: s,
            receiver: r
        }
    }

    fn update_state(&mut self, new_opinion: &Opinion<ObjLD,Peer>)
    {
        // Update the commit estimate.
        self.opinion.estimated_commit =
            &self.opinion.estimated_commit + &new_opinion.estimated_commit;

        // Update the object candidate.
        self.opinion.candidate_object =
            &self.opinion.candidate_object + &new_opinion.candidate_object;

        // Update and trim proposed configs.
        self.opinion.proposed_configs =
            self.opinion.proposed_configs
                .union(&new_opinion.proposed_configs).cloned().collect();
        let commit_cfg: &CfgLE<Peer> = self.opinion.estimated_commit.config();
        self.opinion.proposed_configs =
            self.opinion.proposed_configs.iter()
                .filter(|u| ! (*u <= commit_cfg)).cloned().collect();
    }

    async fn receive(&mut self, m: &Message<ObjLD,Peer>)
    {
        match m
        {
            Message::Request{seq, from, to: _, opinion} =>
            {
                self.update_state(opinion);
                let resp = Message::Response
                {
                    seq: *seq,
                    from: self.id.clone(),
                    to: from.clone(),
                    opinion: self.opinion.clone(),
                };
                self.sender.send(resp).await;
            }
            Message::Response{seq, from, to: _, opinion} =>
            {
                self.update_state(opinion);
                if *seq == self.sequence
                {
                    self.sequence_responses.insert(from.clone());
                }
            }
            Message::Commit(state) =>
            {
                let mut committed_opinion = Opinion::default();
                committed_opinion.estimated_commit = state.clone();
                self.update_state(&committed_opinion);
            }
        }
    }

    // Return a set of configs that represent joins of the estimated
    // commit's config with each possible subset of the proposed configs.
    fn every_possible_config_join(&self) -> BTreeSet<CfgLE<Peer>>
    {
        let mut joins : BTreeSet<CfgLE<Peer>> = BTreeSet::new();
        for sz in 0..=self.opinion.proposed_configs.len()
        {
            for subset in
                self.opinion.proposed_configs.iter().combinations(sz)
            {
                let mut cfg : CfgLE<Peer> =
                    self.opinion.estimated_commit.config().clone();
                for s in subset
                {
                    cfg = cfg + s
                }
                joins.insert(cfg);
            }
        }
        joins
    }

    // Return a config value to commit to: join of all active inputs
    // and the commit estimate's config.
    fn commit_cfg(&self) -> CfgLE<Peer> {
        let mut cfg : CfgLE<Peer> = self.opinion.estimated_commit.config().clone();
        for c in self.opinion.proposed_configs.iter() {
            cfg = cfg + c;
        }
        cfg
    }

    fn commit_state(&self) -> StateLE<ObjLD,Peer>
    {
        StateLE::new_from((self.opinion.candidate_object.clone(),
                           self.commit_cfg()))
    }

    fn advance_seq(&mut self)
    {
        self.sequence += 1;
        self.sequence_responses.clear();
    }

    // Return true if we have received a response from a quorum in every
    // configuration in `cfgs`, otherwise false.
    fn have_quorum(&self, cfgs: &BTreeSet<CfgLE<Peer>>) -> bool
    {
        for cfg in cfgs.iter()
        {
            let members = cfg.members();
            let quorum_size = (members.len() / 2) + 1;
            let members_responded =
                self.sequence_responses.intersection(&members).count();
            if members_responded < quorum_size
            {
                return false;
            }
        }
        true
    }

    pub async fn propose(&mut self, prop: &StateLE<ObjLD,Peer>) -> StateLE<ObjLD,Peer>
    {
        let prop_opinion = Opinion
        {
            estimated_commit: self.opinion.estimated_commit.clone(),
            proposed_configs: singleton_set(prop.config().clone()),
            candidate_object: prop.object().clone()
        };
        self.update_state(&prop_opinion);
        let mut learn_lower_bound : Option<StateLE<ObjLD,Peer>> = None;
        loop
        {
            self.advance_seq();
            let old_opinion = self.opinion.clone();
            let cfgs : BTreeSet<CfgLE<Peer>> = self.every_possible_config_join();
            let all_members = members_of_cfgs(&cfgs);
            for peer in all_members
            {
                let req = Message::Request
                {
                    seq: self.sequence,
                    from: self.id.clone(),
                    to: peer.clone(),
                    opinion: self.opinion.clone()
                };
                self.sender.send(req).await;
            }
            while (old_opinion.estimated_commit.config() ==
                   self.opinion.estimated_commit.config())
                || self.have_quorum(&cfgs)
            {
                self.receive(&self.receiver.recv().await.unwrap()).await
            }

            // Stable configuration.
            if (old_opinion.estimated_commit.config() ==
                self.opinion.estimated_commit.config()) &&
                (old_opinion.proposed_configs ==
                 self.opinion.proposed_configs)
            {
                let cstate = self.commit_state();
                if learn_lower_bound == None
                {
                    learn_lower_bound = Some(cstate.clone())
                }
                // No greater object received.
                if old_opinion.candidate_object == self.opinion.candidate_object
                {
                    let broadcast = Message::Commit(cstate.clone());
                    self.sender.send(broadcast).await;
                    return cstate;
                }
            }
            match learn_lower_bound
            {
                Some(state) if state <= self.opinion.estimated_commit =>
                {
                    // Adopt learned state.
                    return self.opinion.estimated_commit.clone();
                }
                _ => ()
            }
        }
    }
}

// misc helpers

fn singleton_set<T:Ord>(t: T) -> BTreeSet<T> {
    let mut s = BTreeSet::new();
    s.insert(t);
    s
}

fn members_of_cfgs<Peer: Ord+Clone+Debug+Default>(cfgs: &BTreeSet<CfgLE<Peer>>) -> BTreeSet<Peer> {
    let mut u = BTreeSet::<Peer>::new();
    for c in cfgs.iter() {
        u = u.union(&c.members()).cloned().collect()
    }
    u
}
