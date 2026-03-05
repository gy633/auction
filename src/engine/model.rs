use std::{marker::PhantomData, num::NonZeroU64, time::Instant};

use proptest::prelude::*;
use rust_decimal::{Decimal, prelude::FromPrimitive};
use strum::{EnumIter, IntoEnumIterator};

// Default buffer size per order type per symbol
pub const ORDER_BUF_SIZE: usize = 50_000;

#[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
// random u64 should be good enough. If not, use UUID
pub struct Id(u64);
impl Id {
    #[inline]
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct Seq(u64);
impl Seq {
    #[inline]
    pub fn new(seq: u64) -> Self {
        Self(seq)
    }
}

/// Smart constructor for positive, non-zero quantities.
#[derive(Debug, Copy, Clone, Eq, PartialEq, PartialOrd, Ord, Hash)]
pub struct Qty(pub NonZeroU64);
impl Qty {
    #[inline]
    pub fn new(qty: NonZeroU64) -> Self {
        Self(qty)
    }

    #[inline]
    pub fn as_f64(self) -> f64 {
        self.0.get() as f64
    }
}

#[cfg(test)]
use std::ops::Add;
#[cfg(test)]
impl Add for Qty {
    type Output = Qty;

    fn add(self, rhs: Self) -> Self::Output {
        Qty::new(unsafe { NonZeroU64::new_unchecked(self.0.get() + rhs.0.get()) })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Side {
    Ask,
    Bid,
}

pub fn side_strategy() -> impl Strategy<Value = Side> {
    prop_oneof![Just(Side::Ask), Just(Side::Bid)]
}

#[derive(Debug, Clone)]
pub struct Order<T> {
    pub id: Id,
    pub seq: Seq,
    pub qty: Qty,
    pub price: Price,
    pub ingress_ts: Instant,
    _side: PhantomData<T>,
}

impl<T> Order<T> {
    #[inline]
    pub fn new(id: Id, seq: Seq, qty: Qty, price: Price, ingress_ts: Instant) -> Self {
        Order {
            id,
            seq,
            qty,
            price,
            ingress_ts,
            _side: PhantomData,
        }
    }
}

pub fn order_strategy<T>(id: Id, seq: Seq) -> impl Strategy<Value = Order<T>>
where
    T: std::fmt::Debug,
{
    // NOTE: The solver gets massively slower the larger the prices get
    (1u64..1000, 100..200u64).prop_map(move |(qty, price)| Order {
        id,
        seq,
        qty: Qty::new(NonZeroU64::new(qty).unwrap()),
        price: Price::new(Decimal::from_u64(price).unwrap()),
        ingress_ts: Instant::now(),
        _side: std::marker::PhantomData,
    })
}

// Used as tags for Order<T>
#[derive(Debug, Clone)]
pub struct Ask;
#[derive(Debug, Clone)]
pub struct Bid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Price(pub Decimal);
impl Price {
    #[inline]
    pub fn new(ticks: Decimal) -> Self {
        Self(ticks)
    }
}

// TODO: Why not reuse Order struct?
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Fill {
    pub id: Id,
    pub qty: Qty,
    pub price: Price,
    pub side: Side,
}

impl Fill {
    #[inline]
    pub fn new(id: Id, qty: Qty, price: Price, side: Side) -> Self {
        Self {
            id,
            qty,
            price,
            side,
        }
    }
}

#[derive(Debug, Clone, Copy, EnumIter, Hash, PartialEq, Eq)]
#[repr(usize)]
pub enum Symbol {
    AAPL = 0,
    MSFT,
    NVDA,
}

pub fn symbol_strategy() -> impl Strategy<Value = Symbol> {
    prop::sample::select(Symbol::iter().collect::<Vec<_>>())
}

#[cfg(test)]
pub mod tests {
    use std::collections::HashMap;

    use proptest::prelude::{Strategy, prop};
    use rustc_hash::FxBuildHasher;

    use crate::engine::model::{Ask, Bid, Id, Order, Seq, Side, order_strategy, side_strategy};

    // This was first used to collect orders before streaming building the LP.
    #[derive(Debug)]
    pub struct Book {
        pub bids: Vec<Order<Bid>>,
        pub asks: Vec<Order<Ask>>,
        bids_hm: HashMap<Id, usize, FxBuildHasher>,
        asks_hm: HashMap<Id, usize, FxBuildHasher>,
    }

    impl Book {
        /// Creates a new [`Book`] with `capacity` for asks and bids, each.
        pub fn with_capacity(capacity: usize) -> Self {
            Self {
                bids: Vec::with_capacity(capacity),
                asks: Vec::with_capacity(capacity),
                bids_hm: HashMap::with_capacity_and_hasher(capacity, FxBuildHasher),
                asks_hm: HashMap::with_capacity_and_hasher(capacity, FxBuildHasher),
            }
        }

        // We assume ids are generated to be unique
        pub fn insert_bid(&mut self, order: Order<Bid>) {
            let id = order.id;
            self.bids.push(order);
            self.bids_hm.insert(id, self.bids.len() - 1);
        }

        pub fn insert_ask(&mut self, order: Order<Ask>) {
            let id = order.id;
            self.asks.push(order);
            self.asks_hm.insert(id, self.asks.len() - 1);
        }

        // untested
        pub fn remove_bid(&mut self, id: Id) -> Option<Order<Bid>> {
            let idx = self.bids_hm.remove(&id)?;
            if idx >= self.bids.len() {
                None
            } else {
                let last_id = self.bids.last()?.id;
                let removed = self.bids.swap_remove(idx);
                self.bids_hm.insert(last_id, idx);

                Some(removed)
            }
        }

        // untested
        pub fn remove_ask(&mut self, id: Id) -> Option<Order<Ask>> {
            let idx = self.asks_hm.remove(&id)?;
            if idx >= self.asks.len() {
                None
            } else {
                let last_id = self.asks.last()?.id;
                let removed = self.asks.swap_remove(idx);
                self.asks_hm.insert(last_id, idx);

                Some(removed)
            }
        }

        pub fn len_bids(&self) -> usize {
            self.bids.len()
        }

        pub fn len_asks(&self) -> usize {
            self.asks.len()
        }

        pub fn len(&self) -> usize {
            self.len_asks() + self.len_bids()
        }

        pub fn is_empty(&self) -> bool {
            self.len() == 0
        }

        pub fn clear(&mut self) {
            self.asks.clear();
            self.bids.clear();
            self.asks_hm.clear();
            self.bids_hm.clear();
        }

        pub fn get_ask_by_id(&self, id: Id) -> Option<&Order<Ask>> {
            let idx = self.asks_hm.get(&id)?;
            let order = &self.asks[*idx];
            assert_eq!(id, order.id);

            Some(order)
        }

        pub fn get_bid_by_id(&self, id: Id) -> Option<&Order<Bid>> {
            let idx = self.bids_hm.get(&id)?;
            let order = &self.bids[*idx];
            assert_eq!(id, order.id);

            Some(order)
        }
    }

    pub fn book_strategy() -> impl Strategy<Value = Book> {
        prop::collection::hash_set(1u64..=100_000, 1..=10_000)
            .prop_flat_map(|ids| {
                let per_id_orders: Vec<_> = ids
                    .into_iter()
                    .map(|id| {
                        side_strategy().prop_flat_map(move |side| match side {
                            Side::Bid => order_strategy(Id::new(id), Seq::new(id))
                                .prop_map(Ok)
                                .boxed(),

                            Side::Ask => order_strategy(Id::new(id), Seq::new(id))
                                .prop_map(Err)
                                .boxed(),
                        })
                    })
                    .collect();

                per_id_orders
            })
            .prop_map(|orders| {
                let mut book = Book::with_capacity(orders.len());

                for order in orders {
                    match order {
                        Ok(bid) => book.insert_bid(bid),
                        Err(ask) => book.insert_ask(ask),
                    }
                }

                book
            })
    }
}
