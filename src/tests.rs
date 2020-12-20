// Copyright 2020 Graydon Hoare <graydon@pobox.com>
// Licensed under the MIT and Apache-2.0 licenses.

use crate::*;
use log::debug;
use pergola::{LatticeElt, MaxDef};
use pretty_env_logger;
use std::collections::BTreeMap;

type Peer = String;
type ObjLD = MaxDef<u16>;
type ObjLE = LatticeElt<ObjLD>;
type Msg = Message<ObjLD, Peer>;

struct PeerRecord {
    incoming: Vec<Msg>,
    participant: Participant<ObjLD, Peer>,
}

#[derive(Default)]
struct Network {
    peers: BTreeMap<Peer, PeerRecord>,
    num_finished: usize,
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
            p.participant.propose_step(p.incoming.iter(), &mut outgoing);
            p.incoming.clear();
            let is_fini = p.participant.propose_is_fini();
            if !was_fini && is_fini {
                debug!(
                    "peer {} final_state {:?}",
                    p.participant.id, p.participant.final_state
                );
                self.num_finished += 1;
            }
        }
        for msg in outgoing {
            match &msg {
                Message::Request { seq, from, to, .. }
                | Message::Response { seq, from, to, .. } => {
                    let n = if let Message::Request { .. } = msg {
                        "request"
                    } else {
                        "response"
                    };
                    match self.peers.get_mut(to) {
                        None => (),
                        Some(p) => {
                            debug!("point-to-point send {} #{} {} -> {}", n, seq, from, to);
                            p.incoming.push(msg.clone())
                        }
                    }
                }
                Message::Commit { from, .. } => {
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
    let _ = pretty_env_logger::try_init();
    let mut n = Network::default();
    n.add_peer("a".into());
    n.add_peer("b".into());
    n.add_peer("c".into());
    n.run();
}

#[test]
fn manual_cfg_lattice_order() {
    let cfg_default = CfgLE::<u8>::default();
    let mut cfg_elts = cfg_default.clone();
    cfg_elts.added_peers_mut().insert(1);
    assert!(cfg_default <= cfg_elts);
}

#[test]
fn manual_peerset_lattice_order() {
    let ps_default = crate::cfg::PeerSetLE::<u8>::default();
    let mut ps_elts = ps_default.clone();
    ps_elts.value.insert(1);
    assert!(ps_default <= ps_elts);
    assert!(!(ps_default >= ps_elts));
}
