// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

use crate::{CfgLE, CfgLEExt, Message, Opinion, StateLE, StateLEExt};
use itertools::Itertools;
use log::{debug, trace};
use pergola::LatticeDef;
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};

// Participants _could_ be implemented using async/await (and indeed earlier
// versions were) but doing so makes it impossible to capture, inspect, clone,
// and otherwise explicitly-manipulate the state-machine variables that hold the
// progression of the `propose` operation. In particular, we want to hook
// Participants up to explicit-state model checkers, which pretty much requires
// snapshotting state. Oh well.
//
// So instead, we explicitly represent the state-machine of `propose` as an
// enum and manually code all the state transitions.
//
// Every time propose is stepped forwards, it consumes any messages in its
// incoming queue, pushes some number of messages on its outgoing queue, and
// possibly performs a `ProposeStage` transition.
#[derive(Clone, Debug, Hash)]
pub enum ProposeStage {
    Init, // Newly-constructed participant; propose has not yet begun.
    Send, // First part of proposal loop (possibly looping from Pick).
    Recv, // Second part of proposal loop, awaiting stabilization.
    Pick, // Decide whether to finish with a value, or loop.
    Fini, // Finished with a value.
}

/// `Participant`s represent parties participating in lattice agreement. They
/// support a low-level, explicit-state-machine interface that must be manually
/// initialized and stepped, with each step consuming input messages, producing
/// output messages, and possibly performing a state transition. When complete,
/// a final proposal-agreement value can be read out of the Participant.
///
/// Note that unlike in a full consensus system, the response from `propose` is
/// _not_ guaranteed to be equal at all `Participant`s, nor is it guaranteed to
/// even be a value any `Participant` proposed.
///
/// (The exact guarantees on the proposal, its response, and its relationships
///  to other proposals and responses on other peers are somewhat subtle, see
///  the paper.)
///
/// Responsibility for routing `Message::Request`s and `Message::Response`s to
/// their destination `Peer`s is left to the client of this library, as is
/// responsibility for distributing broadcast `Message::Commit`s.
///
/// All such communication is assumed to be (eventually) reliable, though no
/// timing assumptions are made. Lost messages, or a sufficient number of
/// quorum-peer failures, may cause `Participant`s to get stuck. If this is
/// a concern, the state machine should be embedded in a higher-level retry
/// protocol that uses timeouts to ensure eventual delivery.

#[derive(Debug)]
pub struct Participant<ObjLD: LatticeDef, Peer: Ord + Clone + Debug + Hash>
where
    ObjLD::T: Clone + Debug + Default,
{
    // State variables that can persist across proposals.
    pub id: Peer,
    sequence: u64,
    opinion: Opinion<ObjLD, Peer>,

    // State variables reset once per proposal.
    propose_stage: ProposeStage,
    learn_lower_bound: Option<StateLE<ObjLD, Peer>>,
    pub final_state: Option<StateLE<ObjLD, Peer>>,

    // State variables reset once per send-recv-pick loop.
    old_opinion: Opinion<ObjLD, Peer>,
    all_possible_cfgs: BTreeSet<CfgLE<Peer>>,
    sequence_responses: BTreeSet<Peer>,
}

impl<ObjLD: LatticeDef, Peer: Ord + Clone + Debug + Hash> std::clone::Clone
    for Participant<ObjLD, Peer>
where
    ObjLD::T: Clone + Debug + Default,
{
    fn clone(&self) -> Self {
        Participant {
            propose_stage: self.propose_stage.clone(),
            sequence: self.sequence,
            opinion: self.opinion.clone(),
            learn_lower_bound: self.learn_lower_bound.clone(),
            final_state: self.final_state.clone(),
            old_opinion: self.old_opinion.clone(),
            all_possible_cfgs: self.all_possible_cfgs.clone(),
            sequence_responses: self.sequence_responses.clone(),
            id: self.id.clone(),
        }
    }
}

impl<ObjLD: LatticeDef, Peer: Ord + Clone + Debug + Hash> Hash for Participant<ObjLD, Peer>
where
    ObjLD::T: Clone + Debug + Default + Hash,
{
    fn hash<H: Hasher>(&self, hstate: &mut H) {
        self.propose_stage.hash(hstate);
        self.sequence.hash(hstate);
        self.opinion.hash(hstate);
        self.learn_lower_bound.hash(hstate);
        self.final_state.hash(hstate);
        self.old_opinion.hash(hstate);
        self.all_possible_cfgs.hash(hstate);
        self.sequence_responses.hash(hstate);
        self.id.hash(hstate);
    }
}

impl<ObjLD: LatticeDef + 'static, Peer: Ord + Clone + Debug + Hash + 'static>
    Participant<ObjLD, Peer>
where
    ObjLD::T: Clone + Debug + Default,
{
    pub fn new(id: Peer) -> Self {
        Participant {
            id: id,
            sequence: 0,
            opinion: Opinion::default(),

            propose_stage: ProposeStage::Init,
            learn_lower_bound: None,
            final_state: None,

            old_opinion: Opinion::default(),
            all_possible_cfgs: BTreeSet::new(),
            sequence_responses: BTreeSet::new(),
        }
    }

    fn update_state(&mut self, new_opinion: &Opinion<ObjLD, Peer>) {
        // Update the commit estimate.
        self.opinion.estimated_commit =
            &self.opinion.estimated_commit + &new_opinion.estimated_commit;

        // Update the object candidate.
        self.opinion.candidate_object =
            &self.opinion.candidate_object + &new_opinion.candidate_object;

        // Update and trim proposed configs.
        self.opinion.proposed_configs = self
            .opinion
            .proposed_configs
            .union(&new_opinion.proposed_configs)
            .cloned()
            .collect();
        let commit_cfg: &CfgLE<Peer> = self.opinion.estimated_commit.config();
        self.opinion.proposed_configs = self
            .opinion
            .proposed_configs
            .iter()
            .filter(|u| !(*u <= commit_cfg))
            .cloned()
            .collect();
    }

    fn receive(&mut self, m: &Message<ObjLD, Peer>, outgoing: &mut Vec<Message<ObjLD, Peer>>) {
        match m {
            Message::Request {
                seq, from, opinion, ..
            } => {
                self.update_state(opinion);
                let resp = Message::Response {
                    seq: *seq,
                    from: self.id.clone(),
                    to: from.clone(),
                    opinion: self.opinion.clone(),
                };
                outgoing.push(resp);
            }
            Message::Response {
                seq, from, opinion, ..
            } => {
                self.update_state(opinion);
                if *seq == self.sequence {
                    self.sequence_responses.insert(from.clone());
                }
            }
            Message::Commit { state, .. } => {
                let mut committed_opinion = Opinion::default();
                committed_opinion.estimated_commit = state.clone();
                self.update_state(&committed_opinion);
            }
        }
    }

    // Return a set of configs that represent joins of the estimated
    // commit's config with each possible subset of the proposed configs.
    fn every_possible_config_join(&self) -> BTreeSet<CfgLE<Peer>> {
        let mut joins: BTreeSet<CfgLE<Peer>> = BTreeSet::new();
        for sz in 0..=self.opinion.proposed_configs.len() {
            for subset in self.opinion.proposed_configs.iter().combinations(sz) {
                let mut cfg: CfgLE<Peer> = self.opinion.estimated_commit.config().clone();
                for s in subset {
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
        let mut cfg: CfgLE<Peer> = self.opinion.estimated_commit.config().clone();
        for c in self.opinion.proposed_configs.iter() {
            cfg = cfg + c;
        }
        cfg
    }

    fn commit_state(&self) -> StateLE<ObjLD, Peer> {
        StateLE::new_from((self.opinion.candidate_object.clone(), self.commit_cfg()))
    }

    fn advance_seq(&mut self) {
        self.sequence += 1;
        trace!("peer {:?} advanced seq to #{}", self.id, self.sequence);
        self.sequence_responses.clear();
    }

    // Return true if we have received a response from a quorum in every
    // configuration in `cfgs`, otherwise false.
    fn have_quorum_in_all_possible_cfgs(&self) -> bool {
        for cfg in self.all_possible_cfgs.iter() {
            let members = cfg.members();
            let quorum_size = (members.len() / 2) + 1;
            let members_responded = self.sequence_responses.intersection(&members).count();
            let members_concurring = 1 /*self*/ + members_responded;
            if members_concurring < quorum_size {
                trace!("peer {:?} does not have quorum", self.id);
                return false;
            }
        }
        trace!("peer {:?} has quorum", self.id);
        true
    }

    fn send_request_to_peer(&mut self, peer: Peer, outgoing: &mut Vec<Message<ObjLD, Peer>>) {
        if peer == self.id {
            return;
        }
        let req = Message::Request {
            seq: self.sequence,
            from: self.id.clone(),
            to: peer.clone(),
            opinion: self.opinion.clone(),
        };
        outgoing.push(req);
    }

    fn propose_send(&mut self, outgoing: &mut Vec<Message<ObjLD, Peer>>) {
        self.advance_seq();
        self.old_opinion = self.opinion.clone();
        self.all_possible_cfgs = self.every_possible_config_join();
        let all_members = members_of_cfgs(&self.all_possible_cfgs);
        for peer in all_members {
            self.send_request_to_peer(peer, outgoing);
        }
        self.propose_stage = ProposeStage::Recv;
    }

    fn propose_recv(&mut self) {
        if !self.opinion.same_estimated_commit_config(&self.old_opinion)
            || self.have_quorum_in_all_possible_cfgs()
        {
            self.propose_stage = ProposeStage::Pick;
        }
    }

    fn propose_pick(&mut self, outgoing: &mut Vec<Message<ObjLD, Peer>>) {
        if self
            .opinion
            .same_estimated_and_proposed_configs(&self.old_opinion)
        {
            debug!("peer {:?} found stable configuration", self.id);
            let cstate = self.commit_state();
            if self.learn_lower_bound == None {
                self.learn_lower_bound = Some(cstate.clone())
            }
            // No greater object received.
            if self.old_opinion.candidate_object == self.opinion.candidate_object {
                debug!(
                    "peer {:?} stable config has stable object, broadcasting and finishing",
                    self.id
                );
                let broadcast = Message::Commit {
                    from: self.id.clone(),
                    state: cstate.clone(),
                };
                outgoing.push(broadcast);
                self.final_state = Some(cstate);
                self.propose_stage = ProposeStage::Fini;
                return;
            }
        }
        match &self.learn_lower_bound {
            Some(state) if state <= &self.opinion.estimated_commit => {
                // Adopt learned state.
                debug!(
                    "peer {:?} stable config has acceptable lower bound, finishing",
                    self.id
                );
                self.final_state = Some(self.opinion.estimated_commit.clone());
                self.propose_stage = ProposeStage::Fini;
                return;
            }
            _ => (),
        }
        // Made progress but unable to finish, transition back to send.
        self.propose_stage = ProposeStage::Send;
    }

    pub fn propose_step<'a, MI>(&mut self, incoming: MI, outgoing: &mut Vec<Message<ObjLD, Peer>>)
    where
        MI: std::iter::Iterator<Item = &'a Message<ObjLD, Peer>>,
    {
        for msg in incoming {
            self.receive(msg, outgoing);
        }
        match self.propose_stage {
            ProposeStage::Init => (),
            ProposeStage::Send => self.propose_send(outgoing),
            ProposeStage::Recv => self.propose_recv(),
            ProposeStage::Pick => self.propose_pick(outgoing),
            ProposeStage::Fini => (),
        }
    }

    /// Returns true iff the participant's proposal state-machine is fini.
    pub fn propose_is_fini(&self) -> bool {
        match self.propose_stage {
            ProposeStage::Fini => true,
            _ => false,
        }
    }

    /// Starts a new proposal, resetting necessary state, so can be called at any time,
    pub fn propose(&mut self, prop: &StateLE<ObjLD, Peer>) {
        // Reset proposal state variables.
        self.learn_lower_bound = None;
        self.final_state = None;
        self.old_opinion = Opinion::default();
        self.all_possible_cfgs = BTreeSet::new();
        self.sequence_responses = BTreeSet::new();

        // Integrate proposal into internal state.
        let prop_opinion = Opinion {
            estimated_commit: self.opinion.estimated_commit.clone(),
            proposed_configs: singleton_set(prop.config().clone()),
            candidate_object: prop.object().clone(),
        };
        self.update_state(&prop_opinion);

        // Transition to send stage (start of send/recv/pick loop).
        self.propose_stage = ProposeStage::Send;
    }
}
// misc helpers

fn singleton_set<T: Ord>(t: T) -> BTreeSet<T> {
    let mut s = BTreeSet::new();
    s.insert(t);
    s
}

fn members_of_cfgs<Peer: Ord + Clone + Debug + Hash>(
    cfgs: &BTreeSet<CfgLE<Peer>>,
) -> BTreeSet<Peer> {
    let mut u = BTreeSet::<Peer>::new();
    for c in cfgs.iter() {
        u = u.union(&c.members()).cloned().collect()
    }
    u
}