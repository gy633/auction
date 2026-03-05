use std::collections::HashMap;
use std::time::Instant;

use hdrhistogram::Histogram;
use nexus_channel::{Receiver, Sender, TryRecvError};
use proptest::prelude::*;
use rand::Rng;
use rustc_hash::FxBuildHasher;
use tracing::{error, info, instrument, warn};

use crate::{
    engine::model::{
        Ask, Bid, Id, Order, Seq, Side, Symbol, order_strategy, side_strategy, symbol_strategy,
    },
    service::{matcher, scheduler::Scheduler},
};

#[derive(Debug)]
pub enum Message {
    NewAsk((Symbol, Order<Ask>)),
    NewBid((Symbol, Order<Bid>)),
    // Cancel not implemented
    CancelOrder(Id),
}

// abuse proptest to generate messages in the ingress
pub fn new_order_message_strategy(id: Id, seq: Seq) -> impl Strategy<Value = Message> {
    (symbol_strategy(), side_strategy()).prop_flat_map(move |(symbol, side)| match side {
        Side::Ask => order_strategy(id, seq)
            .prop_map(move |order| Message::NewAsk((symbol, order)))
            .boxed(),
        Side::Bid => order_strategy(id, seq)
            .prop_map(move |order| Message::NewBid((symbol, order)))
            .boxed(),
    })
}

#[instrument(skip(rx_ingress, tx_matcher, rng, sequencer_hist))]
pub fn run<R>(
    mut rx_ingress: Receiver<Message>,
    mut tx_matcher: Sender<matcher::Message>,
    rng: R,
    shard: Vec<Symbol>,
    sequencer_hist: &mut Histogram<u64>,
) where
    R: Rng,
{
    info!("Sequencer thread running.");

    let mut work_done = false;
    let mut scheduler = Scheduler::new(rng, shard.len());
    for symbol in &shard {
        scheduler.schedule_auction_for(*symbol);
    }

    // for a small enough shard, just using a Vec<(Symbol, Book)> and scan it should be faster
    let mut auctions_to_run = HashMap::with_capacity_and_hasher(shard.len(), FxBuildHasher);
    // TODO: use them for reporting, actually.
    let mut sequence_numbers: HashMap<Symbol, u64, FxBuildHasher> =
        shard.iter().map(|symbol| (*symbol, 0)).collect();

    loop {
        let latency;

        scheduler.auctions_to_run(&mut auctions_to_run);
        for symbol in auctions_to_run.keys() {
            scheduler.schedule_auction_for(*symbol);
        }

        let start = Instant::now();
        let msg = rx_ingress.try_recv();
        match msg {
            Ok(msg) => {
                match msg {
                    Message::NewAsk((symbol, order)) => {
                        maybe_run_auction(
                            &order.ingress_ts,
                            &mut auctions_to_run,
                            symbol,
                            &mut tx_matcher,
                        );

                        let seq = sequence_numbers
                            .get_mut(&symbol)
                            .expect("Ingress only sends us symbols we handle.");
                        if let Err(e) = tx_matcher.send(matcher::Message::NewAsk((symbol, order))) {
                            error!("Error sending ask from sequencer to matcher : {e}");
                        }
                        *seq += 1;

                        latency = start.elapsed().as_nanos() as u64;
                    }
                    Message::NewBid((symbol, order)) => {
                        maybe_run_auction(
                            &order.ingress_ts,
                            &mut auctions_to_run,
                            symbol,
                            &mut tx_matcher,
                        );

                        let seq = sequence_numbers
                            .get_mut(&symbol)
                            .expect("Ingress only sends us symbols we handle.");
                        if let Err(e) = tx_matcher.send(matcher::Message::NewBid((symbol, order))) {
                            error!("Error sending bid from sequencer to matcher : {e}");
                        }
                        *seq += 1;

                        latency = start.elapsed().as_nanos() as u64;
                    }
                    Message::CancelOrder(_id) => todo!(),
                }
                work_done = true;

                if let Err(e) = sequencer_hist.record(latency) {
                    error!("Cannot add record to sequencer histogram: {e}")
                }
            }

            Err(TryRecvError::Empty) => (),
            // ingress died
            Err(TryRecvError::Disconnected) => return,
        }

        if !work_done {
            std::hint::spin_loop()
        }

        work_done = false;
    }
}

fn maybe_run_auction(
    ingress_ts: &Instant,
    auctions_to_run: &mut HashMap<Symbol, Instant, FxBuildHasher>,
    symbol: Symbol,
    tx_matcher: &mut Sender<matcher::Message>,
) {
    match auctions_to_run.get(&symbol) {
        None => (),
        Some(auction_start_time) => {
            if *ingress_ts > *auction_start_time {
                if let Err(e) = tx_matcher.send(matcher::Message::RunAuction(symbol)) {
                    error!(
                        "Error sending run auction for {symbol:?} from sequencer to matcher : {e}"
                    );
                }

                auctions_to_run.remove(&symbol);
            }
        }
    }
}
