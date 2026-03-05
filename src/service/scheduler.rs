use std::collections::HashMap;
use std::time::Duration;
use std::time::Instant;

use rand::Rng;
use rand::RngExt;
use rustc_hash::FxBuildHasher;

use crate::engine::model::Symbol;

// Start auction at the earlierst in 50ms
const LOWER_BOUND_MS: u64 = 50;
// Start auction at the latest in 100ms
const UPPER_BOUND_MS: u64 = 100;

pub struct Scheduler<R: Rng> {
    rng: R,
    start: Instant,
    next_auctions: HashMap<Symbol, Duration, FxBuildHasher>,
}

impl<R: Rng> Scheduler<R> {
    pub fn new(rng: R, n_symbols: usize) -> Self {
        Self {
            rng,
            start: Instant::now(),
            next_auctions: HashMap::with_capacity_and_hasher(n_symbols, FxBuildHasher),
        }
    }

    pub fn schedule_auction_for(&mut self, symbol: Symbol) {
        let next_auction_time = self.start.elapsed()
            + Duration::from_millis(self.rng.random_range(LOWER_BOUND_MS..=UPPER_BOUND_MS));

        self.next_auctions
            .entry(symbol)
            .and_modify(|next_auction| *next_auction = next_auction_time)
            .or_insert(next_auction_time);
    }

    pub fn auctions_to_run(&mut self, symbols: &mut HashMap<Symbol, Instant, FxBuildHasher>) {
        let elapsed_delta = self.start.elapsed();
        for (symbol, auction_delta) in self.next_auctions.iter() {
            if elapsed_delta >= *auction_delta {
                symbols.insert(*symbol, self.start + *auction_delta);
            }
        }
    }
}
