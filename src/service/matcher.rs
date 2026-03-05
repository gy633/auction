use std::{collections::HashMap, time::Instant};

use hdrhistogram::Histogram;
use nexus_channel::{Receiver, TryRecvError};
use rustc_hash::FxBuildHasher;
use tracing::{error, info, instrument, warn};

use crate::engine::{
    matcher::stream::highs::Highs,
    model::{Ask, Bid, Id, ORDER_BUF_SIZE, Order, Symbol},
};

#[derive(Debug)]
pub enum Message {
    NewAsk((Symbol, Order<Ask>)),
    NewBid((Symbol, Order<Bid>)),
    RunAuction(Symbol),
    // Cancel not implemented
    CancelOrder(Id),
}

#[instrument(skip(rx_sequencer, matcher_hist))]
pub fn run(
    shard: Vec<Symbol>,
    mut rx_sequencer: Receiver<Message>,
    matcher_hist: &mut Histogram<u64>,
) {
    info!("Matcher thread running.");

    let mut fills = Vec::with_capacity(ORDER_BUF_SIZE);
    let mut auction_id = 0;
    let mut work_done = false;

    // for a small enough shard, just using a Vec<(Symbol, Book)> and scan it should be faster
    let mut matchers: HashMap<Symbol, Highs, FxBuildHasher> = shard
        .into_iter()
        .map(|symbol| {
            (
                symbol,
                Highs::try_with_capacity(ORDER_BUF_SIZE * 2)
                    .expect("If we cannot even initialise HiGHS, there's nothing we can do."),
            )
        })
        .collect();

    loop {
        match rx_sequencer.try_recv() {
            Ok(Message::NewAsk((symbol, order))) => {
                matchers
                    .get_mut(&symbol)
                    .expect("Sequencer only sends Symbols we handle.")
                    .insert_ask(&order);
                work_done = true;
            }
            Ok(Message::NewBid((symbol, order))) => {
                matchers
                    .get_mut(&symbol)
                    .expect("Sequencer only sends Symbols we handle.")
                    .insert_bid(&order);
                work_done = true;
            }
            Ok(Message::RunAuction(symbol)) => {
                info!("Running auction for {symbol:?}");
                let match_start = Instant::now();
                let matcher = matchers
                    .get_mut(&symbol)
                    .expect("Sequencer only sends Symbols we handle.");
                matcher.match_orders(auction_id, &mut fills);

                let latency = match_start.elapsed().as_nanos() as u64;
                if let Err(e) = matcher_hist.record(latency) {
                    error!("Cannot add record to matcher histogram: {e}")
                }

                // TODO: Send these fills somewhere for processing
                info!("fills {}", fills.len());
                auction_id += 1;

                work_done = true;
            }
            Ok(Message::CancelOrder(_id)) => todo!(),
            Err(TryRecvError::Empty) => (),
            Err(TryRecvError::Disconnected) => return,
        }

        if !work_done {
            std::hint::spin_loop()
        }

        work_done = false;
    }
}
