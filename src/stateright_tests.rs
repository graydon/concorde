// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

use stateright::actor::*;
use stateright::actor::system::*;
use stateright::checker::*;

type ObjLD = pergola::MaxDef<u16>;
type Msg = crate::Message<ObjLD,Id>;
type Participant = crate::Participant<ObjLD,Id>;

struct ConcordeActor;

impl Actor for ConcordeActor {
    type Msg = Msg;
    type State = Participant;

    fn init(i: InitIn<Self>, o: &mut Out<Self>)
    {
        o.set_state(Participant::new(i.id));
    }

    fn next(i: NextIn<Self>, o: &mut Out<Self>)
    {
        let Event::Receive(_, msg) = i.event;
        let incoming: Vec<Msg> = vec![msg];
        let mut outgoing: Vec<Msg> = vec![];
        let mut st = i.state.clone();
        st.propose_step(incoming.iter(), &mut outgoing);
        o.set_state(st);
        for m in outgoing
        {
            match m
            {
                super::Message::Request{to, ..} => o.send(to, m),
                super::Message::Response{to, ..} => o.send(to, m),
                super::Message::Commit{..} => (),
            }
        }
    }
}

fn system() -> System<ConcordeActor>
{
    let actors = vec![
        ConcordeActor{}
    ];
    System::with_actors(actors)
}

fn model(sys: System<ConcordeActor>)
    -> Model<'static, System<ConcordeActor>>
{
        Model {
            state_machine: sys,
            properties: vec![],
            boundary: None
        }
}

#[cfg(test)]
#[test]
fn model_check()
{
    let mut checker = model(system()).checker();
    assert_eq!(checker.check(1000).is_done(), true);
}
