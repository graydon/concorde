// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

/*!
 * This crate is a work in progress attempt to implement a small, special-case
 * consensus-like algorithm called "reconfigurable lattice agreement", which has
 * some desirable properties:
 *
 *   - It's asynchronous, small, fast and simple compared with other
 *     consensus-like algorithms. Few states, message types and round-trips. The
 *     paper introducing it describes it in 20 lines of pseudocode.
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

// TODO: timeouts?
// TODO: lots more testing.
// TODO: add mechanism to approve/disapprove of specific quorums.
// TODO: add a trim watermark to CfgLD so it's not ever-growing.

use pergola::*;
use std::collections::{BTreeSet};
use std::fmt::Debug;
use itertools::Itertools;
use log::{debug,trace};

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
impl<ObjLD: LatticeDef,
     Peer: Ord+Clone+Debug+Default>
    Opinion<ObjLD,Peer>
where ObjLD::T : Clone+Debug+Default
{
    fn same_estimated_commit_config(&self,
                                    other: &Self) -> bool
    {
        self.estimated_commit.config() ==
            other.estimated_commit.config()
    }
    fn same_estimated_and_proposed_configs(&self,
                                           other: &Self) -> bool
    {
        self.same_estimated_commit_config(other) &&
            self.proposed_configs == other.proposed_configs
    }
}

// Manually implement Default since #[derive(Default)] fails here,
// see bug https://github.com/rust-lang/rust/issues/26925
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

// Manually implement Clone since #[derive(Clone)] fails here,
// see bug https://github.com/rust-lang/rust/issues/26925
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

/// Messages are either unidirectional point-to-point requests
/// or responses (matched by sequence number) or broadcast
/// commit messages sent to all peers.
#[derive(Debug)]
pub enum Message<ObjLD: LatticeDef,
                 Peer: Ord+Clone+Debug+Default>
where ObjLD::T : Clone+Debug+Default
{
    Request{seq: u64, from: Peer, to: Peer, opinion: Opinion<ObjLD,Peer>},
    Response{seq: u64, from: Peer, to: Peer, opinion: Opinion<ObjLD,Peer>},
    Commit{from: Peer, state: StateLE<ObjLD,Peer>}
}

// Manually implement Clone since #[derive(Clone)] fails here,
// see bug https://github.com/rust-lang/rust/issues/26925
impl<ObjLD: LatticeDef,
     Peer: Ord+Clone+Debug+Default>
    std::clone::Clone
    for Message<ObjLD,Peer>
where ObjLD::T : Clone+Debug+Default
{
    fn clone(&self) -> Self
    {
        match self {
            Message::Request{seq,from,to,opinion} =>
                Message::Request{seq:*seq,from:from.clone(),
                                 to:to.clone(),opinion:opinion.clone()},
            Message::Response{seq,from,to,opinion} =>
                Message::Response{seq:*seq,from:from.clone(),
                                  to:to.clone(),opinion:opinion.clone()},
            Message::Commit{from,state} => Message::Commit{from:from.clone(),
                                                           state:state.clone()}
        }
    }
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
// incoming queue, leaves some number of messages in its outgoing queue, and
// possibly performs a `ProposeStage` transition.
#[derive(Clone)]
pub enum ProposeStage
{
    Init, // Newly-constructed participant; propose has not yet begun.
    Send, // First part of proposal loop (possibly looping from Pick).
    Recv, // Second part of proposal loop, awaiting stabilization.
    Pick, // Decide whether to finish with a value, or loop.
    Fini, // Finished with a value.
}

pub struct Participant<ObjLD: LatticeDef,
                       Peer: Ord+Clone+Debug+Default>
where ObjLD::T : Clone+Debug+Default
{
    // State variables that can persist across proposals.
    id: Peer,
    sequence: u64,
    opinion: Opinion<ObjLD,Peer>,

    // State variables reset once per proposal.
    propose_stage: ProposeStage,
    learn_lower_bound: Option<StateLE<ObjLD,Peer>>,
    final_state: Option<StateLE<ObjLD,Peer>>,

    // State variables reset once per send-recv-pick loop.
    old_opinion: Opinion<ObjLD,Peer>,
    all_possible_cfgs: BTreeSet<CfgLE<Peer>>,
    sequence_responses: BTreeSet<Peer>,
}

impl<ObjLD: LatticeDef,
     Peer: Ord+Clone+Debug+Default>
    std::clone::Clone
    for Participant<ObjLD,Peer>
where ObjLD::T : Clone+Debug+Default
{
    fn clone(&self) -> Self
    {
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

impl<ObjLD:LatticeDef+'static,
     Peer:Ord+Clone+Debug+Default+'static>
    Participant<ObjLD, Peer>
where ObjLD::T : Clone+Debug+Default
{
    pub fn new(id: Peer) -> Self
    {
        Participant
        {
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

    fn receive(&mut self, m: &Message<ObjLD,Peer>,
               outgoing: &mut Vec<Message<ObjLD,Peer>>)
    {
        match m
        {
            Message::Request{seq, from, opinion, ..} =>
            {
                self.update_state(opinion);
                let resp = Message::Response
                {
                    seq: *seq,
                    from: self.id.clone(),
                    to: from.clone(),
                    opinion: self.opinion.clone(),
                };
                outgoing.push(resp);
            }
            Message::Response{seq, from, opinion, ..} =>
            {
                self.update_state(opinion);
                if *seq == self.sequence
                {
                    self.sequence_responses.insert(from.clone());
                }
            }
            Message::Commit{state, ..} =>
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
        trace!("peer {:?} advanced seq to #{}", self.id, self.sequence);
        self.sequence_responses.clear();
    }

    // Return true if we have received a response from a quorum in every
    // configuration in `cfgs`, otherwise false.
    fn have_quorum_in_all_possible_cfgs(&self) -> bool
    {
        for cfg in self.all_possible_cfgs.iter()
        {
            let members = cfg.members();
            let quorum_size = (members.len() / 2) + 1;
            let members_responded =
                self.sequence_responses.intersection(&members).count();
            let members_concurring = 1 /*self*/ + members_responded;
            if members_concurring < quorum_size
            {
                trace!("peer {:?} does not have quorum", self.id);
                return false;
            }
        }
        trace!("peer {:?} has quorum", self.id);
        true
    }

    fn send_request_to_peer(&mut self, peer: Peer,
                            outgoing: &mut Vec<Message<ObjLD,Peer>>)
    {
        if peer == self.id {
            return;
        }
        let req = Message::Request
        {
            seq: self.sequence,
            from: self.id.clone(),
            to: peer.clone(),
            opinion: self.opinion.clone()
        };
        outgoing.push(req);
    }

    fn propose_send(&mut self,
                    outgoing: &mut Vec<Message<ObjLD,Peer>>)
    {
        self.advance_seq();
        self.old_opinion = self.opinion.clone();
        self.all_possible_cfgs = self.every_possible_config_join();
        let all_members = members_of_cfgs(&self.all_possible_cfgs);
        for peer in all_members
        {
            self.send_request_to_peer(peer, outgoing);
        }
        self.propose_stage = ProposeStage::Recv;
    }

    fn propose_recv(&mut self)
    {
        if !self.opinion.same_estimated_commit_config(&self.old_opinion)
            || self.have_quorum_in_all_possible_cfgs()
        {
            self.propose_stage = ProposeStage::Pick;
        }
    }

    fn propose_pick(&mut self, outgoing: &mut Vec<Message<ObjLD,Peer>>)
    {
        if self.opinion.same_estimated_and_proposed_configs(&self.old_opinion)
        {
            debug!("peer {:?} found stable configuration", self.id);
            let cstate = self.commit_state();
            if self.learn_lower_bound == None
            {
                self.learn_lower_bound = Some(cstate.clone())
            }
            // No greater object received.
            if self.old_opinion.candidate_object == self.opinion.candidate_object
            {
                debug!("peer {:?} stable config has stable object, broadcasting and finishing", self.id);
                let broadcast = Message::Commit{from: self.id.clone(), state: cstate.clone()};
                outgoing.push(broadcast);
                self.final_state = Some(cstate);
                self.propose_stage = ProposeStage::Fini;
                return;
            }
        }
        match &self.learn_lower_bound
        {
            Some(state) if state <= &self.opinion.estimated_commit =>
            {
                // Adopt learned state.
                debug!("peer {:?} stable config has acceptable lower bound, finishing", self.id);
                self.final_state = Some(self.opinion.estimated_commit.clone());
                self.propose_stage = ProposeStage::Fini;
                return;
            }
            _ => ()
        }
        // Made progress but unable to finish, transition back to send.
        self.propose_stage = ProposeStage::Send;
    }

    pub fn propose_step<'a, MI>(&mut self, incoming: MI,
                                outgoing: &mut Vec<Message<ObjLD,Peer>>)
    where MI: std::iter::Iterator<Item = &'a Message<ObjLD,Peer>>
    {
        for msg in incoming {
            self.receive(msg, outgoing);
        }
        match self.propose_stage
        {
            ProposeStage::Init => (),
            ProposeStage::Send => self.propose_send(outgoing),
            ProposeStage::Recv => self.propose_recv(),
            ProposeStage::Pick => self.propose_pick(outgoing),
            ProposeStage::Fini => (),
        }
    }

    /// Returns true iff the participant's proposal state-machine is fini.
    pub fn propose_is_fini(&mut self) -> bool
    {
        match self.propose_stage {
            ProposeStage::Fini => true,
            _ => false
        }
    }

    /// Starts a new proposal, resetting necessary state, so can be called at any time,
    pub fn propose(&mut self, prop: &StateLE<ObjLD,Peer>)
    {
        // Reset proposal state variables.
        self.learn_lower_bound = None;
        self.final_state = None;
        self.old_opinion = Opinion::default();
        self.all_possible_cfgs = BTreeSet::new();
        self.sequence_responses = BTreeSet::new();

        // Integrate proposal into internal state.
        let prop_opinion = Opinion
        {
            estimated_commit: self.opinion.estimated_commit.clone(),
            proposed_configs: singleton_set(prop.config().clone()),
            candidate_object: prop.object().clone()
        };
        self.update_state(&prop_opinion);

        // Transition to send stage (start of send/recv/pick loop).
        self.propose_stage = ProposeStage::Send;
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

#[cfg(test)]
mod tests {
    use pretty_env_logger;
    use super::*;
    use pergola::MaxDef;
    use std::collections::BTreeMap;

    type Peer = String;
    type ObjLD = MaxDef<u16>;
    type ObjLE = LatticeElt<ObjLD>;
    type Msg = Message<ObjLD,Peer>;

    struct PeerRecord {
        incoming: Vec<Msg>,
        participant: Participant<ObjLD,Peer>,
    }

    #[derive(Default)]
    struct Network {
        peers: BTreeMap<Peer,PeerRecord>,
        num_finished: usize
    }

    impl Network {
        fn add_peer(&mut self, id: Peer) {
            let p = Participant::new(id);
            let pr = PeerRecord {
                incoming: vec![],
                participant: p,
            };
            self.peers.insert(pr.participant.id.clone(), pr);
        }

        fn all_finished(&self) -> bool {
            self.num_finished == self.peers.len()
        }

        fn step(&mut self) {
            let mut outgoing: Vec<Msg> = vec![];
            for p in self.peers.values_mut() {
                let was_fini = p.participant.propose_is_fini();
                p.participant.propose_step(p.incoming.iter(),
                                           &mut outgoing);
                p.incoming.clear();
                let is_fini = p.participant.propose_is_fini();
                if !was_fini && is_fini  {
                    debug!("peer {} final_state {:?}", p.participant.id,
                           p.participant.final_state);
                    self.num_finished += 1;
                }
            }
            for msg in outgoing {
                match &msg {
                    Message::Request{seq, from, to, ..} |
                    Message::Response{seq, from, to, ..}
                    =>
                    {
                        let n = if let Message::Request{..} = msg
                        { "request" } else { "response"};
                        match self.peers.get_mut(to) {
                            None => (),
                            Some(p) => {
                                debug!("point-to-point send {} #{} {} -> {}",
                                       n, seq, from, to);
                                p.incoming.push(msg.clone())
                            }
                        }
                    }
                    Message::Commit{from,..} =>
                    {
                        for (id, p) in self.peers.iter_mut() {
                            debug!("broadcast {} send to {}", from, id);
                            p.incoming.push(msg.clone());
                        }
                    }
                }
            }
        }

        fn run(&mut self) {
            let mut cfg = CfgLE::default();
            for id in self.peers.keys() {
                debug!("created peer {}", id);
                cfg.added_peers_mut().insert(id.clone());
            }
            let mut obj = ObjLE::new_from(1000);
            for p in self.peers.values_mut() {
                obj.value += 1;
                let state = StateLE::new_from((obj.clone(), cfg.clone()));
                p.participant.propose(&state);
            }
            while !self.all_finished() {
                self.step();
            }
        }
    }

    #[test]
    fn run_sim() {
        pretty_env_logger::init();
        let mut n = Network::default();
        n.add_peer("a".into());
        n.add_peer("b".into());
        n.add_peer("c".into());
        n.run();
    }
}
