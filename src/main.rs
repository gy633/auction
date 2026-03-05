use std::thread;

use core_affinity::CoreId;
use hdrhistogram::Histogram;
use nexus_channel::channel;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use strum::IntoEnumIterator;
use tracing::{error, info};
use tracing_appender::non_blocking;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use crate::{
    engine::model::Symbol,
    service::{ingress, matcher, sequencer},
};

pub mod engine;
pub mod service;

fn main() {
    let _log_guard = setup_logger();

    let (ingress_tx, sequencer_rx) = channel::<sequencer::Message>(16384);
    let (sequencer_tx, matcher_rx) = channel::<matcher::Message>(8);

    // TODO: make sure we only get hardware cores so we can just use the vec
    // instead of hardcoded numbers...
    // On my machine, core 0, 1, 2, and 3 are distint CPU cores
    let core_ids = core_affinity::get_core_ids().unwrap();
    assert!(core_ids.len() >= 3);

    let hdl_matcher = thread::spawn(|| {
        if !core_affinity::set_for_current(CoreId { id: 0 }) {
            panic!("Couldn't matcher thread pin to core")
        }

        let mut matcher_hist = new_histogram();
        matcher::run(Symbol::iter().collect(), matcher_rx, &mut matcher_hist);

        matcher_hist
    });

    // We just use one shard for simplicity
    let hdl_sequencer = thread::spawn(|| {
        if !core_affinity::set_for_current(CoreId { id: 1 }) {
            panic!("Couldn't pin sequencer thread to core")
        }
        let mut sequencer_hist = new_histogram();
        // Just to test and get some latency numbers out to compare between runs/changes
        let seed: u64 = 42;
        let rng = ChaCha8Rng::seed_from_u64(seed);

        sequencer::run(
            sequencer_rx,
            sequencer_tx,
            rng,
            Symbol::iter().collect(),
            &mut sequencer_hist,
        );

        sequencer_hist
    });

    let hdl_ingress = thread::spawn(|| {
        if !core_affinity::set_for_current(CoreId { id: 2 }) {
            panic!("Couldn't pin ingress thread to core")
        }
        let mut ingress_hist = new_histogram();
        ingress::run(ingress_tx, &mut ingress_hist);

        ingress_hist
    });

    let real_ingress_hist = match hdl_ingress.join() {
        Ok(h) => h,
        Err(e) => {
            error!("Error joining ingress handle: {e:?}");
            return;
        }
    };

    let sequencer_hist = match hdl_sequencer.join() {
        Ok(h) => h,
        Err(e) => {
            error!("Error joining sequencer handle: {e:?}");
            return;
        }
    };

    let matcher_hist = match hdl_matcher.join() {
        Ok(h) => h,
        Err(e) => {
            error!("Error joining matcher handle: {e:?}");
            return;
        }
    };

    info!("ingress latencies (ns):");
    info!("min: {}", real_ingress_hist.min());
    info!("p50: {}", real_ingress_hist.value_at_percentile(50.0));
    info!("p99: {}", real_ingress_hist.value_at_percentile(99.0));
    info!("p99.9: {}", real_ingress_hist.value_at_percentile(99.9));
    info!("p99.99: {}", real_ingress_hist.value_at_percentile(99.99));
    info!("p99.999: {}", real_ingress_hist.value_at_percentile(99.999));
    info!("max: {}", real_ingress_hist.max());

    info!("sequencer latencies (ns):");
    info!("min: {}", sequencer_hist.min());
    info!("p50: {}", sequencer_hist.value_at_percentile(50.0));
    info!("p99: {}", sequencer_hist.value_at_percentile(99.0));
    info!("p99.9: {}", sequencer_hist.value_at_percentile(99.9));
    info!("p99.99: {}", sequencer_hist.value_at_percentile(99.99));
    info!("p99.999: {}", sequencer_hist.value_at_percentile(99.999));
    info!("max: {}", sequencer_hist.max());

    info!("matcher latencies (ns):");
    info!("min: {}", matcher_hist.min());
    info!("p50: {}", matcher_hist.value_at_percentile(50.0));
    info!("p99: {}", matcher_hist.value_at_percentile(99.0));
    info!("p99.9: {}", matcher_hist.value_at_percentile(99.9));
    info!("p99.99: {}", matcher_hist.value_at_percentile(99.99));
    info!("p99.999: {}", matcher_hist.value_at_percentile(99.999));
    info!("max: {}", matcher_hist.max());
}

fn new_histogram() -> Histogram<u64> {
    // We know this configuration works so we can unwrap() safely
    Histogram::<u64>::new_with_bounds(1, 1_000_000_000, 3).unwrap()
}

fn setup_logger() -> non_blocking::WorkerGuard {
    let (non_blocking_stdout, guard) = non_blocking(std::io::stdout());

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().with_writer(non_blocking_stdout))
        .init();

    info!("Logger setup complete.");
    guard
}
