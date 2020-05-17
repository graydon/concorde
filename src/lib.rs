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

mod cfg;
mod state;
mod opinion;
mod message;
mod participant;

pub use cfg::{CfgLD,CfgLE,CfgLEExt};
pub use state::{StateLD,StateLE,StateLEExt};
pub use message::Message;
pub use opinion::Opinion;
pub use participant::Participant;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod stateright_tests;
