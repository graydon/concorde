// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

use crate::{CfgLE, CfgLEExt, Message, Opinion, Participant, StateLE, StateLEExt};
use bit_set::BitSet;
use im::OrdSet as ArcOrdSet;
use pergola::BitSetWrapper;
use pretty_env_logger;
use stateright::actor::system::*;
use stateright::actor::*;
use stateright::checker::*;
use stateright::*;
use std::collections::{BTreeMap, BTreeSet};
use tracing::debug;

type ObjLD = pergola::BitSetWithUnion;
type ObjLE = pergola::LatticeElt<ObjLD>;
type Msg = Message<ObjLD, Id>;
type Part = Participant<ObjLD, Id>;

struct ConcordeActor {
    // This 'id' field seems a little redundant but we need it to fish the id of
    // an actor out of the System<> struct, which doesn't otherwise provide a
    // way to fetch an actor's id in a property-check.
    id: Id,
    proposals_to_make: Vec<StateLE<ObjLD, Id>>,
}

fn step_participant(participant: &mut Part, incoming: &Vec<Msg>, o: &mut Out<ConcordeActor>) {
    let mut outgoing: Vec<Msg> = vec![];
    participant.propose_step(incoming.iter(), &mut outgoing);
    for m in outgoing {
        match m {
            Message::Request { to, .. } => o.send(to, m),
            Message::Response { to, .. } => o.send(to, m),
            Message::Commit { from, state } => {
                let cm = Message::Commit {
                    from: from,
                    state: state.clone(),
                };
                for id in state.config().members().iter() {
                    o.send(*id, cm.clone())
                }
            }
        }
    }
}

impl ConcordeActor {
    // We make a proposal on a Participant when its previous proposal has
    // completed and there's a proposal remaining in the ConcordeActor (self)
    // context that hasn't been proposed by the Participant yet.
    fn next_proposal(&self, part: &Part) -> Option<StateLE<ObjLD, Id>> {
        let nextprop: usize = part.proposed_history.len();
        // If we're in the middle of a proposal, make no new proposals.
        if nextprop != part.learned_history.len() {
            return None;
        }
        // If we're done making proposals, make no new proposals.
        if nextprop >= self.proposals_to_make.len() {
            return None;
        }
        return Some(self.proposals_to_make[nextprop].clone());
    }
}

impl Actor for ConcordeActor {
    type Msg = Msg;
    type State = Part;
    fn on_start(&self, id: Id, o: &mut Out<Self>) {
        debug!("initializing {:?}", id);

        // Silly assert: Actor ids are assigned by us, separately
        // from stateright's assignment of on_start ids, but they
        // need to match up.
        assert!(id == self.id);
        let mut participant = Part::new(id);
        if let Some(p) = &self.next_proposal(&participant) {
            debug!("proposing on {:?}", id);
            participant.propose(p);
            step_participant(&mut participant, &vec![], o);
        }
        o.set_state(participant);
    }

    fn on_msg(&self, id: Id, state: &Self::State, _src: Id, msg: Self::Msg, o: &mut Out<Self>) {
        debug!("next event on {:?}: recv {:?}", id, msg);
        let mut st = state.clone();
        if let Some(p) = &self.next_proposal(&st) {
            debug!("proposing on {:?}", id);
            st.propose(p);
        }
        step_participant(&mut st, &vec![msg], o);
        o.set_state(st);
    }
}

struct ConcordeSystem {
    peer_proposals: BTreeMap<Id, Vec<StateLE<ObjLD, Id>>>,
}

impl ConcordeSystem {
    fn simple() -> ConcordeSystem {
        let mut m = BTreeMap::new();
        let mut cfg = CfgLE::default();
        let id0 = Id::from(0);
        let id1 = Id::from(1);
        let id2 = Id::from(2);
        cfg.added_peers_mut().insert(id0);
        cfg.added_peers_mut().insert(id1);
        cfg.added_peers_mut().insert(id2);

        let obj1 = ObjLE::new_from(BitSetWrapper(BitSet::from_bytes(&[0b1000_0000])));
        let obj2 = ObjLE::new_from(BitSetWrapper(BitSet::from_bytes(&[0b0100_0000])));

        m.insert(id0, vec![]);
        m.insert(id1, vec![StateLE::new_from((obj1.clone(), cfg.clone()))]);
        m.insert(id2, vec![StateLE::new_from((obj2.clone(), cfg.clone()))]);
        ConcordeSystem { peer_proposals: m }
    }
}

impl System for ConcordeSystem {
    type Actor = ConcordeActor;
    fn actors(&self) -> Vec<Self::Actor> {
        self.peer_proposals
            .clone()
            .into_iter()
            .map(|(id, proposals_to_make)| ConcordeActor {
                id,
                proposals_to_make,
            })
            .collect()
    }
    fn lossy_network(&self) -> LossyNetwork {
        LossyNetwork::No
    }
    fn duplicating_network(&self) -> DuplicatingNetwork {
        DuplicatingNetwork::No
    }
    fn properties(&self) -> Vec<Property<SystemModel<Self>>> {
        vec![
            Property::always("valid", lattice_agreement_validity),
            Property::always("consistent", lattice_agreement_consistency),
            Property::eventually("live", lattice_agreement_liveness),
            Property::always("trivial", |_, _| true),
        ]
    }
    fn within_boundary(&self, state: &SystemState<Self::Actor>) -> bool {
        lattice_agreement_boundary(self, state)
    }
}

// Properties to check:
//
// - validity: if propose(v) returns v', then v' is a join of other proposed values
//   including v and all values learned before propose(v)
// - consistency: learned values are totally ordered by <=
// - liveness: every propose eventually finishes with a learn

fn lattice_agreement_validity(
    _model: &SystemModel<ConcordeSystem>,
    state: &SystemState<ConcordeActor>,
) -> bool {
    let mut all_proposed_added_peers = BTreeSet::new();
    let mut all_proposed_removed_peers = BTreeSet::new();
    let mut all_proposed_bits = BitSet::new();
    for part in state.actor_states.iter() {
        for v in part.proposed_history.iter() {
            let cfg = v.config();
            for peer in cfg.added_peers().iter() {
                all_proposed_added_peers.insert(peer.clone());
            }
            for peer in cfg.removed_peers().iter() {
                all_proposed_removed_peers.insert(peer.clone());
            }
            for bit in v.object().value.0.iter() {
                all_proposed_bits.insert(bit);
            }
        }
    }

    for part in state.actor_states.iter() {
        for v in part.learned_history.iter() {
            let cfg = v.config();
            for peer in cfg.added_peers().iter() {
                if !all_proposed_added_peers.contains(peer) {
                    return false;
                }
            }
            for peer in cfg.removed_peers().iter() {
                if !all_proposed_removed_peers.contains(peer) {
                    return false;
                }
            }
            for bit in v.object().value.0.iter() {
                if !all_proposed_bits.contains(bit) {
                    return false;
                }
            }
        }
    }
    true
}

fn lattice_agreement_consistency(
    _model: &SystemModel<ConcordeSystem>,
    state: &SystemState<ConcordeActor>,
) -> bool {
    for part in state.actor_states.iter() {
        let h = &part.learned_history;
        for i in 1..h.len() {
            if h[i - 1] < h[i] {
                return false;
            }
        }
    }
    return true;
}

// Stateright's support for liveness properties is somewhat limited: it can only
// check finite and acyclic "eventually" properties (I added support for them
// last week!), meaning we're really checking that a given terminal state in a
// (finite, acyclic) path is an acceptable place to stop. So this function just
// checks if we're "finished" when we stop: all proposals we _wanted_ to make
// were made, and they all _completed_ with a learned value.
//
// (Luckily this is how the liveness of lattice agreement is specified)
fn lattice_agreement_liveness(
    model: &SystemModel<ConcordeSystem>,
    state: &SystemState<ConcordeActor>,
) -> bool {
    for part in state.actor_states.iter() {
        for actor in model.actors.iter() {
            if actor.id == part.id {
                let n = actor.proposals_to_make.len();
                if !(part.proposed_history.len() == n && part.learned_history.len() == n) {
                    return false;
                }
            }
        }
    }
    return true;
}

// Returning false here means "past the boundary, stop exploring"
fn lattice_agreement_boundary(system: &ConcordeSystem, state: &SystemState<ConcordeActor>) -> bool {
    for part in state.actor_states.iter() {
        match system.peer_proposals.get(&part.id) {
            None => (),
            Some(proposals) => {
                let n = proposals.len();
                if part.proposed_history.len() < n || part.learned_history.len() < n {
                    // 'true' means there's still work to do in at least one actor.
                    return true;
                }
            }
        }
    }
    // 'false' means every actor has done what they set out to do.

    //println!("----");
    //println!("hit boundary with fingerprint {:08x}", stateright::fingerprint(state));
    //print_system_state(state);

    return false;
}

#[cfg(test)]
#[test]
fn model_check() {
    let _ = pretty_env_logger::try_init();
    let mut checker = ConcordeSystem::simple()
        .into_model()
        .checker_with_threads(num_cpus::get());

    // Oddly, due to a bug in cargo we shouldn't write to stdout because
    // it won't be swallowed by the test runner when running multithreaded
    // (see https://github.com/rust-lang/rust/issues/42474) so we write
    // to a local buffer and then println!() it ourselves.
    let mut buf: Vec<u8> = Vec::new();
    checker.check_and_report(&mut buf);

    // Actually the printout from stateright is illegible so we just throw
    // out their result and print our own.
    // println!("{}", std::str::from_utf8(&buf).unwrap());

    for prop in vec!["valid", "consistent", "live"] {
        match checker.counterexample(prop) {
            None => println!("property '{}' holds", prop),
            Some(cex) => {
                println!("found counterexample to property '{}'", prop);
                print_path(cex);
            }
        }
    }
}

//---------------------------------------------------------
// Remainder is just for pretty-printing paths
//---------------------------------------------------------

fn obj_to_string(obj: &ObjLE) -> String {
    let mut v: u8 = 0;
    for i in obj.value.0.iter() {
        v |= 1_u8 << i;
    }
    format!("{:#08b}", v)
}

fn responses_to_string(resp: &ArcOrdSet<Id>) -> String {
    let mut s = String::new();
    s += "{";
    let mut first = true;
    for peer in resp.iter() {
        if !first {
            s += ",";
        }
        first = false;
        s += &usize::from(*peer).to_string();
    }
    s += "}";
    s
}

fn cfg_to_string(cfg: &CfgLE<Id>) -> String {
    let mut s = String::new();
    s += "{";
    let mut first = true;
    for peer in cfg.members() {
        if !first {
            s += ",";
        }
        first = false;
        s += &usize::from(peer).to_string();
    }
    s += "}";
    s
}

fn cfgs_to_string(cfgs: &ArcOrdSet<CfgLE<Id>>) -> String {
    let mut s = String::new();
    s += "{";
    let mut first = true;
    for cfg in cfgs.iter() {
        if !first {
            s += ",";
        }
        first = false;
        s += &cfg_to_string(cfg);
    }
    s += "}";
    s
}

fn state_to_string(state: &StateLE<ObjLD, Id>) -> String {
    format!(
        "[obj:{}, cfg:{}]",
        obj_to_string(state.object()),
        cfg_to_string(state.config())
    )
}

fn state_opt_to_string(state: &Option<StateLE<ObjLD, Id>>) -> String {
    match state {
        None => String::from("None"),
        Some(s) => state_to_string(s),
    }
}

fn opinion_to_string(op: &Opinion<ObjLD, Id>) -> String {
    format!(
        "[est_commit: {}, prop_cfgs:{}, cand_obj:{}]",
        state_to_string(&op.estimated_commit),
        cfgs_to_string(&op.proposed_configs),
        obj_to_string(&op.candidate_object)
    )
}

fn msg_to_string(msg: &Msg) -> String {
    match msg {
        Message::Request { seq, opinion, .. } => {
            format!("Req?(seq={}, opinion={})", seq, opinion_to_string(opinion))
        }
        Message::Response { seq, opinion, .. } => {
            format!("Res!(seq={}, opinion={})", seq, opinion_to_string(opinion))
        }
        Message::Commit { state, .. } => format!("Commit({})", state_to_string(state)),
    }
}

fn print_system_state(state: &SystemState<ConcordeActor>) {
    // println!("    state at depth {}:", state.depth);
    for part in state.actor_states.iter() {
        println!("        participant {}:", usize::from(part.id));
        println!(
            "            seq: {}, stage: {:?}, seq_responses: {}",
            part.sequence,
            part.propose_stage,
            responses_to_string(&part.sequence_responses)
        );
        println!(
            "            new_opinion: {}",
            opinion_to_string(&part.new_opinion)
        );
        println!(
            "            old_opinion: {}",
            opinion_to_string(&part.old_opinion)
        );
        println!(
            "            lower_bound: {}",
            state_opt_to_string(&part.lower_bound)
        );
        println!(
            "            final_state: {}",
            state_opt_to_string(&part.final_state)
        );
        println!(
            "            proposed {}, learned {}",
            part.proposed_history.len(),
            part.learned_history.len()
        );
    }
    println!("    network:");
    for envelope in state.network.iter() {
        println!(
            "        message {} -> {}: {}",
            usize::from(envelope.src),
            usize::from(envelope.dst),
            msg_to_string(&envelope.msg)
        );
    }
}

fn print_system_action_opt(action: &Option<SystemAction<Msg>>) {
    match action {
        None => println!("        None"),
        Some(SystemAction::Timeout(_)) => println!("        timeout"),
        Some(SystemAction::Drop(envelope)) => println!(
            "        drop message {} -> {}: {}",
            usize::from(envelope.src),
            usize::from(envelope.dst),
            msg_to_string(&envelope.msg)
        ),
        Some(SystemAction::Deliver { src, dst, msg }) => println!(
            "        recv message {} -> {}: {}",
            usize::from(*src),
            usize::from(*dst),
            msg_to_string(&msg)
        ),
    }
}

fn print_path(path: Path<SystemState<ConcordeActor>, SystemAction<Msg>>) {
    for (i, (state, action)) in path.into_vec().iter().enumerate() {
        println!("----");
        println!("step {}:", i);
        print_system_state(state);
        println!("    action:");
        print_system_action_opt(action);
    }
}
