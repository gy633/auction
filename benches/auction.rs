use std::num::NonZeroU64;
use std::time::Instant;

use criterion::{Criterion, criterion_group, criterion_main};

use auction::engine::matcher::stream::highs::Highs;
use auction::engine::model::{Ask, Bid, Id, Order, Price, Qty, Seq};
use rust_decimal::dec;

fn make_bids(n: usize) -> Vec<Order<Bid>> {
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        v.push(Order::new(
            Id::new(i as u64),
            Seq::new(i as u64),
            Qty::new(NonZeroU64::new(100).unwrap()),
            Price::new(dec!(10.10)),
            Instant::now(),
        ));
    }
    v
}

fn make_asks(n: usize) -> Vec<Order<Ask>> {
    let mut v = Vec::with_capacity(n);
    for i in 0..n {
        v.push(Order::new(
            Id::new(i as u64),
            Seq::new(i as u64),
            Qty::new(NonZeroU64::new(100).unwrap()),
            Price::new(dec!(10.00)),
            Instant::now(),
        ));
    }
    v
}

fn bench_matcher_only(c: &mut Criterion) {
    let n = 5000;
    let bids = make_bids(std::hint::black_box(n));
    let asks = make_asks(std::hint::black_box(n));

    let mut fills = Vec::with_capacity(2 * n);
    c.bench_function("matcher_10k_orders", |b| {
        b.iter(|| {
            let mut highs = Highs::try_with_capacity(2 * n).unwrap();
            for ask in &asks {
                highs.insert_ask(ask);
            }
            for bid in &bids {
                highs.insert_bid(bid);
            }

            highs.match_orders(0, &mut fills);
            std::hint::black_box(fills.clear());
        })
    });
}

criterion_group!(benches, bench_matcher_only);
criterion_main!(benches);
