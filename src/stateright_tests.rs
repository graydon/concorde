// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

use crate::{CfgLE, CfgLEExt, Message, Participant, Opinion, StateLE, StateLEExt};
use log::debug;
use pretty_env_logger;
use stateright::*;
use stateright::actor::system::*;
use stateright::actor::*;
use stateright::checker::*;
use std::sync::Arc;
use std::collections::BTreeSet;

type ObjLD = pergola::MaxDef<u16>;
type ObjLE = pergola::LatticeElt<ObjLD>;
type Msg = Message<ObjLD, Id>;
type Part = Participant<ObjLD, Id>;

struct ConcordeActor {
    proposal: Option<StateLE<ObjLD, Id>>,
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

impl Actor for ConcordeActor {
    type Msg = Msg;
    type State = Part;

    fn init(i: InitIn<Self>, o: &mut Out<Self>) {
        debug!("initializing {:?}", i.id);
        let mut participant = Part::new(i.id);
        if let Some(p) = &i.context.proposal {
            debug!("proposing on {:?}", i.id);
            participant.propose(p);
            step_participant(&mut participant, &vec![], o);
        }
        o.set_state(participant);
    }

    fn next(i: NextIn<Self>, o: &mut Out<Self>) {
        let Event::Receive(_, msg) = i.event;
        debug!("next event on {:?}: recv {:?}", i.id, msg);
        let mut st = i.state.clone();
        step_participant(&mut st, &vec![msg], o);
        o.set_state(st);
    }
}

fn system() -> System<ConcordeActor> {
    let mut cfg = CfgLE::default();
    cfg.added_peers_mut().insert(Id::from(0));
    cfg.added_peers_mut().insert(Id::from(1));
    cfg.added_peers_mut().insert(Id::from(2));

    let obj1 = ObjLE::new_from(1000);
    let obj2 = ObjLE::new_from(1001);

    let actors = vec![
        ConcordeActor { proposal: None },
        ConcordeActor {
            proposal: Some(StateLE::new_from((obj1.clone(), cfg.clone()))),
        },
        ConcordeActor {
            proposal: Some(StateLE::new_from((obj2.clone(), cfg.clone()))),
        },
    ];
    System::with_actors(actors)
}

fn model(sys: System<ConcordeActor>) -> Model<'static, System<ConcordeActor>> {
    Model {
        state_machine: sys,
        properties: vec![Property::always(
            "unfinished",
            |_sys: &System<ConcordeActor>, _state: &SystemState<ConcordeActor>|
            _state.actor_states.iter().all(|state:&Arc<Part>| (**state).final_state.is_none()),
        )],
        boundary: None,
    }
}


#[cfg(test)]
#[test]
fn model_check() {
    let _ = pretty_env_logger::try_init();
    let mut checker = model(system()).checker_with_threads(num_cpus::get());
    checker.check_and_report(&mut std::io::stdout());
    match checker.counterexample("unfinished") {
        None => println!("property holds"),
        Some(cex) =>
            {
                println!("found counterexample to property");
                print_path(cex);
            }
    }
}


//---------------------------------------------------------
// Remainder is just for pretty-printing paths
//---------------------------------------------------------

fn obj_to_string(obj: &ObjLE) -> String
{
    obj.value.to_string()
}

fn responses_to_string(resp: &BTreeSet<Id>) -> String
{
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

fn cfg_to_string(cfg: &CfgLE<Id>) -> String
{
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

fn cfgs_to_string(cfgs: &BTreeSet<CfgLE<Id>>) -> String
{
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

fn state_to_string(state: &StateLE<ObjLD, Id>) -> String
{
    format!("[obj:{}, cfg:{}]",
            obj_to_string(state.object()),
            cfg_to_string(state.config()))
}

fn state_opt_to_string(state: &Option<StateLE<ObjLD, Id>>) -> String
{
    match state {
        None => String::from("None"),
        Some(s) => state_to_string(s)
    }
}

fn opinion_to_string(op: &Opinion<ObjLD, Id>) -> String
{
    format!("[est_commit: {}, prop_cfgs:{}, cand_obj:{}]",
            state_to_string(&op.estimated_commit),
            cfgs_to_string(&op.proposed_configs),
            obj_to_string(&op.candidate_object))
}

fn msg_to_string(msg: &Msg) -> String
{
    match msg {
        Message::Request { seq, opinion, .. } =>
            format!("Request(seq={}, opinion={})",
                    seq, opinion_to_string(opinion)),
        Message::Response { seq, opinion, .. } =>
            format!("Response(seq={}, opinion={})",
                    seq, opinion_to_string(opinion)),
        Message::Commit { state, .. } =>
            format!("Commit({})", state_to_string(state))

    }
}


fn print_path(path: Path<<System<ConcordeActor> as StateMachine>::State,
                         <System<ConcordeActor> as StateMachine>::Action>)
{
    for (i, (state, action)) in path.into_vec().iter().enumerate()
    {
        println!("----");
        println!("step {}:", i);
        println!("    state:");
        for part in state.actor_states.iter()
        {
            println!("        participant {}:", usize::from(part.id));
            println!("            seq: {}, stage: {:?}, seq_responses: {}",
                     part.sequence, part.propose_stage,
                     responses_to_string(&part.sequence_responses));
            println!("            new_opinion: {}", opinion_to_string(&part.new_opinion));
            println!("            old_opinion: {}", opinion_to_string(&part.old_opinion));
            println!("            lower_bound: {}", state_opt_to_string(&part.lower_bound));
            println!("            final_state: {}", state_opt_to_string(&part.final_state));
        }
        println!("    network:");
        for envelope in state.network.iter()
        {
            println!("        message {} -> {}: {}",
                     usize::from(envelope.src),
                     usize::from(envelope.dst),
                     msg_to_string(&envelope.msg));
        }
        println!("    action:");
        match action
        {
            None =>
                println!("        None"),
            Some(SystemAction::Drop(envelope)) =>
                println!("        drop message {} -> {}: {}",
                         usize::from(envelope.src),
                         usize::from(envelope.dst),
                         msg_to_string(&envelope.msg)),
            Some(SystemAction::Act(dst, Event::Receive(src, msg))) =>
                println!("        recv message {} -> {}: {}",
                         usize::from(*src),
                         usize::from(*dst),
                         msg_to_string(&msg))
        }
    }
}
