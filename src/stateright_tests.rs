// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

use crate::{CfgLE, CfgLEExt, Message, Participant, StateLE, StateLEExt};
use log::debug;
use pretty_env_logger;
use stateright::actor::system::*;
use stateright::actor::*;
use stateright::checker::*;

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
            "proposed",
            |_sys: &System<ConcordeActor>, _state: &SystemState<ConcordeActor>| true,
        )],
        boundary: None,
    }
}

#[cfg(test)]
#[test]
fn model_check() {
    pretty_env_logger::init();
    let mut checker = model(system()).checker_with_threads(num_cpus::get());
    checker.check_and_report(&mut std::io::stdout());
}
