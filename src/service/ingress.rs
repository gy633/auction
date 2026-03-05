use std::time::Instant;

use hdrhistogram::Histogram;
use nexus_channel::Sender;
use proptest::{
    prelude::Strategy,
    strategy::ValueTree,
    test_runner::{Config, RngAlgorithm, TestRng, TestRunner},
};
use tracing::{error, info, instrument};

use crate::{
    engine::model::{Id, Seq},
    service::sequencer::{Message, new_order_message_strategy},
};

#[instrument(skip_all)]
pub fn run(mut tx_sequencer: Sender<Message>, hist: &mut Histogram<u64>) {
    info!("Ingress thread running");

    let config = Config {
        failure_persistence: None,
        ..Config::default()
    };

    let mut runner = TestRunner::new_with_rng(
        config,
        TestRng::from_seed(
            RngAlgorithm::ChaCha,
            "deadbeef12356789ABCDEFdeadbeef12".as_bytes(),
        ),
    );

    let mut counter = 0;
    let mut id = 0;

    loop {
        let start = Instant::now();
        // hacky test code abusing proptest generation
        let strategy = new_order_message_strategy(Id::new(id), Seq::new(id));
        let tree = strategy.new_tree(&mut runner).unwrap();
        let msg = tree.current();

        if let Err(e) = tx_sequencer.send(msg) {
            error!("Error sending from ingress to sequencer: {e}");
        }
        let latency = start.elapsed().as_nanos() as u64;
        if let Err(e) = hist.record(latency) {
            error!("Cannot add record to ingress histogram: {e}")
        }

        if counter > 10_000_000 {
            info!("Exiting ingress thread.");
            return;
        }

        counter += 1;
        id += 1;
    }
}
