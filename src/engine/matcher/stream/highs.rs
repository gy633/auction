use std::collections::HashMap;
use std::ffi::c_void;
use std::num::NonZeroU64;
use std::ptr;

use highs_sys::HighsInt;
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rustc_hash::FxBuildHasher;
use tracing::{debug, error, info, instrument, warn};

use crate::engine::model::{Ask, Bid, Fill, Id, Order, Price, Qty, Side};

type Idx = usize;

#[derive(Debug)]
pub struct Highs {
    instance: *mut c_void,

    // The HiGHS buffers
    // The coefficients of the variables.
    // These are the prices of the orders.
    // For asks, it is -price.
    col_cost: Vec<f64>,

    // Upper bounds for our variables.
    // These are the quantities of the orders
    col_upper: Vec<f64>,

    // lower bounds for our variables.
    // Will always be 0.0
    col_lower: Vec<f64>,

    // The buffer to store the values for the solved variables.
    // In our case, these are the filled quantities.
    col_values: Vec<f64>,

    // We have only one constraint.
    row_lower: [f64; 1],
    row_upper: [f64; 1],

    // For the shadow price
    row_dual: [f64; 1],

    // -1.0 for asks and 1.0 for bids
    a_value: Vec<f64>,

    // 0 .. capacity
    a_start: Vec<HighsInt>,

    // Always zero
    a_index: Vec<HighsInt>,

    // Bookkeeping helpers
    // current index to all the HiGHS vectors for the current order
    idx: Idx,

    // a helper mapping for cancellation (unused)
    id_to_idx: HashMap<Id, Idx, FxBuildHasher>,

    // reverse of the above
    idx_to_id: HashMap<Idx, Id, FxBuildHasher>,

    // indices of buyers and sellers for branchless iteration
    buyers_idx: Vec<Idx>,
    sellers_idx: Vec<Idx>,

    capacity: usize,
}

impl Highs {
    /// Creates a new [`Highs`] instance with space for `capacity` orders.
    pub fn try_with_capacity(capacity: usize) -> Result<Self, String> {
        let highs_ptr = unsafe { highs_sys::Highs_create() };
        if highs_ptr.is_null() {
            return Err("Could not create HiGHS instance.".to_string());
        }

        unsafe {
            let output = std::ffi::CString::new("output_flag").expect("Static string is safe.");
            highs_sys::Highs_setBoolOptionValue(highs_ptr, output.as_ptr(), 0);

            let threads = std::ffi::CString::new("threads").expect("Static string is safe.");
            highs_sys::Highs_setBoolOptionValue(highs_ptr, threads.as_ptr(), 1);

            let seed = std::ffi::CString::new("random_seed").expect("Static string is safe.");
            highs_sys::Highs_setBoolOptionValue(highs_ptr, seed.as_ptr(), 42);
        }

        let col_lower = vec![0.0; capacity];
        let col_upper = vec![0.0; capacity];
        let col_cost = vec![0.0; capacity];
        let col_values = vec![0.0; capacity];

        let row_lower = [0.0];
        let row_upper = [0.0];
        let row_dual = [0.0];
        let a_value = vec![0.0; capacity];
        let a_start: Vec<_> = (0..capacity as i32).collect();
        let a_index = vec![0; capacity];

        let id_to_idx = HashMap::with_capacity_and_hasher(capacity, FxBuildHasher);
        let idx_to_id = HashMap::with_capacity_and_hasher(capacity, FxBuildHasher);

        Ok(Self {
            instance: highs_ptr,
            col_cost,
            col_upper,
            col_lower,
            col_values,
            a_value,
            a_start,
            a_index,
            idx: 0,
            id_to_idx,
            idx_to_id,
            row_lower,
            row_upper,
            row_dual,
            sellers_idx: Vec::with_capacity(capacity),
            buyers_idx: Vec::with_capacity(capacity),
            capacity,
        })
    }

    pub fn insert_bid(&mut self, bid: &Order<Bid>) {
        if self.idx >= self.capacity {
            error!("HiGHS matcher instance full and regrow not supported. Dropping bid.");
            return;
        }

        self.id_to_idx.insert(bid.id, self.idx);
        self.idx_to_id.insert(self.idx, bid.id);

        self.col_cost[self.idx] = bid
            .price
            .0
            .to_f64()
            .expect("Every valid price is a valid f64.");
        self.col_upper[self.idx] = bid.qty.as_f64();
        self.a_value[self.idx] = 1.0;

        self.buyers_idx.push(self.idx);

        self.idx += 1;
    }

    pub fn insert_ask(&mut self, ask: &Order<Ask>) {
        if self.idx >= self.capacity {
            error!("HiGHS matcher instance full and regrow not supported. Dropping ask.");
            return;
        }

        self.id_to_idx.insert(ask.id, self.idx);
        self.idx_to_id.insert(self.idx, ask.id);

        self.col_cost[self.idx] = -ask
            .price
            .0
            .to_f64()
            .expect("Every valid price is a valid f64.");
        self.col_upper[self.idx] = ask.qty.as_f64();
        self.a_value[self.idx] = -1.0;

        self.sellers_idx.push(self.idx);

        self.idx += 1;
    }

    // TODO: don't clear everything.
    // Roll unmatched and remainder of partially filled orders forward
    fn clear(&mut self) {
        self.idx = 0;

        self.sellers_idx.clear();
        self.buyers_idx.clear();

        self.id_to_idx.clear();
        self.idx_to_id.clear();
    }

    // TODO: needs more testing
    #[instrument(skip(self, fills))]
    pub fn match_orders(&mut self, auction_id: u64, fills: &mut Vec<Fill>) {
        info!(
            id = auction_id,
            num_orders = self.sellers_idx.len() + self.buyers_idx.len(),
            "Starting auction"
        );

        fills.clear();

        if self.sellers_idx.is_empty() || self.buyers_idx.is_empty() {
            return;
        }

        self.col_values.fill(0.0);

        let n_orders = self.sellers_idx.len() + self.buyers_idx.len();
        self.col_cost[n_orders..].fill(0.0);
        self.col_upper[n_orders..].fill(0.0);
        self.a_value[n_orders..].fill(0.0);

        // It should be faster to create this model once and just update the values afterwards.
        // I'm thinking that using real data (and rolling over unmatched orders),
        // https://ergo-code.github.io/HiGHS/dev/guide/further/#hot-start
        // would probably help. But given that I'm using randomly generated data, I saw it actually being slower
        // than building a fresh model. Would need to investigate more though.
        let status = unsafe {
            highs_sys::Highs_passModel(
                self.instance,
                self.col_lower.len() as highs_sys::HighsInt,
                self.row_lower.len() as highs_sys::HighsInt,
                self.col_lower.len() as highs_sys::HighsInt,
                0, // q_num_nz (LP => no Hessian)
                highs_sys::kHighsMatrixFormatColwise as HighsInt,
                highs_sys::kHighsHessianFormatTriangular as HighsInt, // ignored for LP
                highs_sys::kHighsObjSenseMaximize as HighsInt,
                0.0, // objective offset
                self.col_cost.as_ptr(),
                self.col_lower.as_ptr(),
                self.col_upper.as_ptr(),
                self.row_lower.as_ptr(),
                self.row_upper.as_ptr(),
                self.a_start.as_ptr(),
                self.a_index.as_ptr(),
                self.a_value.as_ptr(),
                ptr::null(), // q_start
                ptr::null(), // q_index
                ptr::null(), // q_value
                ptr::null(), // integrality (continuous LP)
            )
        };
        if status != highs_sys::kHighsStatusOk as HighsInt {
            error!("Couldn't pass model! Status: {status}");
            return;
        }

        let status = unsafe { highs_sys::Highs_run(self.instance) };
        assert_eq!(status, highs_sys::kHighsStatusOk as HighsInt);

        self.col_values.fill(0.0);
        let status = unsafe {
            highs_sys::Highs_getSolution(
                self.instance,
                self.col_values.as_mut_ptr(),
                std::ptr::null_mut(), // col_dual
                std::ptr::null_mut(), // row_value
                self.row_dual.as_mut_ptr(),
            )
        };
        assert_eq!(status, highs_sys::kHighsStatusOk as HighsInt);

        let clearing_price = if self.marginal_trader_exist() {
            let shadow_price = self.row_dual[0];
            debug!("highs shadow price {shadow_price:?}");

            Decimal::from_f64(shadow_price)
        } else {
            let p = self.calculate_midpoint_price();
            debug!("highs midpoint price {p:?}");
            p
        };

        let clearing_price = match clearing_price {
            Some(price) => price,
            // Should only happen when noone gets filled
            None => {
                // too noisy?
                info!("No matches in auction");
                return;
            }
        };

        self.fill_fills(&clearing_price, fills);

        self.clear();
    }

    fn marginal_trader_exist(&self) -> bool {
        self.sellers_idx
            .iter()
            .chain(self.buyers_idx.iter())
            .any(|idx| {
                let filled = self.col_values[*idx];
                let qty = self.col_upper[*idx];
                debug!(
                    "highs filled {} {} qty of id {:?}",
                    filled,
                    qty,
                    self.idx_to_id.get(idx)
                );
                filled > 0.0 && filled < qty
            })
    }

    fn calculate_midpoint_price(&self) -> Option<Decimal> {
        let highest_filled_seller = self
            .sellers_idx
            .iter()
            .filter(|idx| self.col_values[**idx] > 0.0)
            .map(|idx| self.col_cost[*idx])
            // NOTE: we use min_by as the price is stored as -price in self.col_cost for bids
            .min_by(|a, b| a.total_cmp(b))?;

        let lowest_filled_buyer = self
            .buyers_idx
            .iter()
            .filter(|idx| self.col_values[**idx] > 0.0)
            .map(|idx| self.col_cost[*idx])
            .min_by(|a, b| a.total_cmp(b))?;

        // NOTE: -highest_filled_seller because we did min_by on negative numbers
        Decimal::from_f64((-highest_filled_seller + lowest_filled_buyer) / 2.0)
    }

    fn fill_fills(&self, clearing_price: &Decimal, fills: &mut Vec<Fill>) {
        let seller_fills = self
            .sellers_idx
            .iter()
            .map(|idx| (idx, self.col_values[*idx]))
            .filter(|(_, filled)| *filled > 0.0)
            .map(|(idx, filled)| {
                (
                    idx,
                    Qty::new(
                        NonZeroU64::new(filled as u64)
                            .expect("Every non-zero quantity is a valid u64"),
                    ),
                )
            })
            .map(|(idx, qty)| {
                let order_id = self.idx_to_id.get(idx).expect("Mapping works correctly.");
                Fill::new(*order_id, qty, Price::new(*clearing_price), Side::Ask)
            });

        let buyer_fills = self
            .buyers_idx
            .iter()
            .map(|idx| (idx, self.col_values[*idx]))
            .filter(|(_, filled)| *filled > 0.0)
            .map(|(idx, filled)| {
                (
                    idx,
                    Qty::new(
                        NonZeroU64::new(filled as u64)
                            .expect("Every non-zero quantity is a valid u64"),
                    ),
                )
            })
            .map(|(idx, qty)| {
                let order_id = self.idx_to_id.get(idx).expect("Mapping works correctly.");
                Fill::new(*order_id, qty, Price::new(*clearing_price), Side::Bid)
            });

        fills.extend(buyer_fills.chain(seller_fills));
    }
}

impl Drop for Highs {
    fn drop(&mut self) {
        unsafe {
            highs_sys::Highs_destroy(self.instance);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{BTreeSet, HashSet},
        num::NonZeroU64,
        time::Instant,
    };

    use proptest::prelude::*;
    use rand::{RngExt, SeedableRng, rngs::SmallRng};
    use rust_decimal::{Decimal, dec, prelude::FromPrimitive};

    use crate::engine::{
        matcher::{naive, stream::highs::Highs},
        model::tests::{Book, book_strategy},
        model::{Fill, Id, Order, Price, Qty, Seq, Side},
    };

    #[test]
    fn empty_no_matches() {
        let mut fills = vec![];
        let mut highs = Highs::try_with_capacity(0).unwrap();
        highs.match_orders(0, &mut fills);

        assert!(fills.is_empty());
    }

    #[test]
    fn bid_price_eq_ask_price() {
        let mut rng = SmallRng::seed_from_u64(12345);
        let qty = Qty::new(NonZeroU64::new(100).unwrap());

        let price = Price::new(dec!(10.00));
        let id_ask = Id::new(rng.random());
        let id_bid = Id::new(rng.random());
        let seq_ask = Seq::new(0);
        let seq_bid = Seq::new(1);
        let ask = Order::new(id_ask, seq_ask, qty, price, Instant::now());
        let bid = Order::new(id_bid, seq_bid, qty, price, Instant::now());

        let mut fills = vec![];
        let mut highs = Highs::try_with_capacity(2).unwrap();
        highs.insert_ask(&ask);
        highs.insert_bid(&bid);
        highs.match_orders(0, &mut fills);

        let expected_fill_price = price;

        assert_eq!(
            fills.into_iter().collect::<BTreeSet<Fill>>(),
            vec![
                Fill::new(id_ask, qty, expected_fill_price, Side::Ask),
                Fill::new(id_bid, qty, expected_fill_price, Side::Bid),
            ]
            .into_iter()
            .collect()
        );
    }

    #[test]
    fn one_chronos_us_eq_ex1() {
        let mut rng = SmallRng::seed_from_u64(12345);

        let price_a = Price::new(dec!(10.00));
        let price_b = Price::new(dec!(10.01));
        let id_bid = Id::new(rng.random());
        let id_ask = Id::new(rng.random());
        let seq_ask = Seq::new(0);
        let seq_bid = Seq::new(1);
        let qty = Qty::new(NonZeroU64::new(100).unwrap());

        let bid = Order::new(id_bid, seq_bid, qty, price_b, Instant::now());
        let ask = Order::new(id_ask, seq_ask, qty, price_a, Instant::now());

        let mut fills = vec![];
        let mut highs = Highs::try_with_capacity(2).unwrap();
        highs.insert_ask(&ask);
        highs.insert_bid(&bid);
        highs.match_orders(0, &mut fills);

        let expected_fill_price = Price::new(dec!(10.005));

        assert_eq!(
            fills.into_iter().collect::<BTreeSet<Fill>>(),
            vec![
                Fill::new(id_bid, qty, expected_fill_price, Side::Bid),
                Fill::new(id_ask, qty, expected_fill_price, Side::Ask)
            ]
            .into_iter()
            .collect()
        );
    }

    #[test]
    fn one_chronos_us_eq_ex2() {
        let mut rng = SmallRng::seed_from_u64(12345);

        let price_ask = Price::new(dec!(10.00));
        let price_bid = Price::new(dec!(10.01));
        let id_bid1 = Id::new(rng.random());
        let id_bid2 = Id::new(rng.random());
        let id_ask = Id::new(rng.random());
        let seq_ask = Seq::new(0);
        let seq_bid1 = Seq::new(1);
        let seq_bid2 = Seq::new(2);
        let qty = Qty::new(NonZeroU64::new(100).unwrap());

        let bid1 = Order::new(id_bid1, seq_bid1, qty, price_bid, Instant::now());
        let bid2 = Order::new(id_bid2, seq_bid2, qty, price_bid, Instant::now());
        let ask = Order::new(id_ask, seq_ask, qty + qty, price_ask, Instant::now());

        let mut fills = vec![];
        let mut highs = Highs::try_with_capacity(3).unwrap();
        highs.insert_ask(&ask);
        highs.insert_bid(&bid1);
        highs.insert_bid(&bid2);
        highs.match_orders(0, &mut fills);

        let expected_fill_price = Price::new(dec!(10.005));

        assert_eq!(
            fills.into_iter().collect::<BTreeSet<Fill>>(),
            vec![
                Fill::new(id_bid1, qty, expected_fill_price, Side::Bid),
                Fill::new(id_bid2, qty, expected_fill_price, Side::Bid),
                Fill::new(id_ask, qty + qty, expected_fill_price, Side::Ask)
            ]
            .into_iter()
            .collect()
        );
    }

    #[test]
    fn one_chronos_us_eq_ex3() {
        let mut rng = SmallRng::seed_from_u64(12345);

        let price_ask = Price::new(dec!(10.00));
        let price_bid = Price::new(dec!(10.01));
        let id_bid1 = Id::new(rng.random());
        let id_bid2 = Id::new(rng.random());
        let id_ask = Id::new(rng.random());
        let seq_ask = Seq::new(0);
        let seq_bid1 = Seq::new(1);
        let seq_bid2 = Seq::new(2);
        let qty = Qty::new(NonZeroU64::new(100).unwrap());

        let bid1 = Order::new(id_bid1, seq_bid1, qty, price_bid, Instant::now());
        let bid2 = Order::new(id_bid2, seq_bid2, qty, price_bid, Instant::now());
        let ask = Order::new(id_ask, seq_ask, qty, price_ask, Instant::now());

        let mut fills = vec![];
        let mut highs = Highs::try_with_capacity(3).unwrap();
        highs.insert_ask(&ask);
        highs.insert_bid(&bid1);
        highs.insert_bid(&bid2);
        highs.match_orders(0, &mut fills);

        let expected_fill_price = Price::new(dec!(10.005));

        let fills_set = fills.into_iter().collect::<BTreeSet<Fill>>();
        let ask_fill = Fill::new(id_ask, qty, expected_fill_price, Side::Ask);
        let fills_bid1 = vec![
            ask_fill.clone(),
            Fill::new(id_bid1, qty, expected_fill_price, Side::Bid),
        ]
        .into_iter()
        .collect();

        let fills_bid2 = vec![
            ask_fill,
            Fill::new(id_bid2, qty, expected_fill_price, Side::Bid),
        ]
        .into_iter()
        .collect();

        assert!(fills_set == fills_bid1 || fills_set == fills_bid2);
    }

    #[test]
    fn one_chronos_us_eq_ex4() {
        let mut rng = SmallRng::seed_from_u64(12345);

        let price_ask = Price::new(dec!(10.00));
        let price_bid1 = Price::new(dec!(10.02));
        let price_bid2 = Price::new(dec!(10.01));
        let id_bid1 = Id::new(rng.random());
        let id_bid2 = Id::new(rng.random());
        let id_ask = Id::new(rng.random());
        let seq_ask = Seq::new(0);
        let seq_bid1 = Seq::new(1);
        let seq_bid2 = Seq::new(2);
        let qty = Qty::new(NonZeroU64::new(100).unwrap());

        let bid1 = Order::new(id_bid1, seq_bid1, qty, price_bid1, Instant::now());
        let bid2 = Order::new(id_bid2, seq_bid2, qty, price_bid2, Instant::now());
        let ask = Order::new(id_ask, seq_ask, qty, price_ask, Instant::now());

        let mut fills = vec![];
        let mut highs = Highs::try_with_capacity(3).unwrap();
        highs.insert_ask(&ask);
        highs.insert_bid(&bid1);
        highs.insert_bid(&bid2);
        highs.match_orders(0, &mut fills);

        let expected_fill_price = Price::new(dec!(10.01));

        assert_eq!(
            fills.into_iter().collect::<BTreeSet<Fill>>(),
            vec![
                Fill::new(id_ask, qty, expected_fill_price, Side::Ask),
                Fill::new(id_bid1, qty, expected_fill_price, Side::Bid)
            ]
            .into_iter()
            .collect()
        )
    }

    #[test]
    fn one_chronos_us_eq_ex5() {
        let mut rng = SmallRng::seed_from_u64(12345);

        let price_ask = Price::new(dec!(10.00));
        let price_bid = Price::new(dec!(10.01));
        let id_bid1 = Id::new(rng.random());
        let id_bid2 = Id::new(rng.random());
        let id_ask = Id::new(rng.random());
        let seq_ask = Seq::new(0);
        let seq_bid1 = Seq::new(1);
        let seq_bid2 = Seq::new(2);
        let qty = Qty::new(NonZeroU64::new(100).unwrap());

        let bid1 = Order::new(id_bid1, seq_bid1, qty, price_bid, Instant::now());
        let bid2 = Order::new(id_bid2, seq_bid2, qty + qty, price_bid, Instant::now());
        let ask = Order::new(id_ask, seq_ask, qty + qty, price_ask, Instant::now());

        let mut fills = vec![];
        let mut highs = Highs::try_with_capacity(3).unwrap();
        highs.insert_bid(&bid1);
        highs.insert_bid(&bid2);
        highs.insert_ask(&ask);
        highs.match_orders(0, &mut fills);

        let expected_fill_price = Price::new(dec!(10.005));

        assert_eq!(
            fills.into_iter().collect::<BTreeSet<Fill>>(),
            vec![
                Fill::new(id_ask, qty + qty, expected_fill_price, Side::Ask),
                Fill::new(id_bid2, qty + qty, expected_fill_price, Side::Bid)
            ]
            .into_iter()
            .collect()
        )
    }

    #[test]
    fn one_chronos_us_eq_ex6() {
        let mut rng = SmallRng::seed_from_u64(12345);

        let price_bid1 = Price::new(dec!(10.02));
        let price_bid2 = Price::new(dec!(10.04));
        let price_ask1 = Price::new(dec!(10.01));
        let price_ask2 = Price::new(dec!(10.02));
        let id_bid1 = Id::new(rng.random());
        let id_bid2 = Id::new(rng.random());
        let id_ask1 = Id::new(rng.random());
        let id_ask2 = Id::new(rng.random());
        let qty = Qty::new(NonZeroU64::new(100).unwrap());
        let seq_ask1 = Seq::new(0);
        let seq_ask2 = Seq::new(1);
        let seq_bid1 = Seq::new(2);
        let seq_bid2 = Seq::new(3);

        let bid1 = Order::new(id_bid1, seq_bid1, qty, price_bid1, Instant::now());
        let bid2 = Order::new(id_bid2, seq_bid2, qty, price_bid2, Instant::now());
        let ask1 = Order::new(id_ask1, seq_ask1, qty, price_ask1, Instant::now());
        let ask2 = Order::new(id_ask2, seq_ask2, qty, price_ask2, Instant::now());

        let mut fills = vec![];
        let mut highs = Highs::try_with_capacity(4).unwrap();
        highs.insert_ask(&ask1);
        highs.insert_ask(&ask2);
        highs.insert_bid(&bid1);
        highs.insert_bid(&bid2);
        highs.match_orders(0, &mut fills);

        let expected_fill_price = Price::new(dec!(10.02));

        // If we get bid price == ask price to match, this test passes as well.
        assert_eq!(
            fills.into_iter().collect::<BTreeSet<Fill>>(),
            vec![
                Fill::new(id_bid1, qty, expected_fill_price, Side::Bid),
                Fill::new(id_bid2, qty, expected_fill_price, Side::Bid),
                Fill::new(id_ask1, qty, expected_fill_price, Side::Ask),
                Fill::new(id_ask2, qty, expected_fill_price, Side::Ask),
            ]
            .into_iter()
            .collect()
        )
    }

    proptest! {
        #[test]
        fn test_market_physics(book in book_strategy()) {
            let mut errors: Vec<String> = vec![];

            if let Err(e) = test_conservation_of_flow(&book) {
                errors.push(e)
            }

            if let Err(e) = test_no_worse_off(&book) {
                errors.push(e)
            }

            if let Err(e) = test_cash_conservation(&book) {
                errors.push(e)
            }

            if let Err(e) = test_unique_clearing_price(&book) {
                errors.push(e)
            }


            if !errors.is_empty() {
                panic!("Market physics violated: {errors:?}")

            }
        }

        // This one seems to fail because the LP is underspecified.
        // Example: bid B1 10 @ 10.10, bid B2 1 @ 10.10, ask A 10 @ 10.00
        // One solver matches 10 B1 and 10 A;
        // The other solver does 9 B1, 1 B2, and 10 A.
        // ==> this also affects the clearing price as the first one uses the midpoint price
        //     and the second one the shadow price due to marginal traders.
        #[test]
        fn test_highs_eq_naive(book in book_strategy()) {
            let mut fills = vec![];

            let mut highs = Highs::try_with_capacity(book.len()).unwrap();
            for ask in &book.asks {
                highs.insert_ask(ask);

            }
            for bid in &book.bids {
                highs.insert_bid(bid);

            }

            let fills_naive: HashSet<_>= naive::tests::match_orders(0, &book).into_iter().collect();
            highs.match_orders(0, &mut fills);

            let highs_fills_set: HashSet<_>= fills.into_iter().collect();

            assert_eq!(highs_fills_set, fills_naive);
        }
    }

    fn test_conservation_of_flow(book: &Book) -> Result<(), String> {
        let mut fills = vec![];

        let mut highs = Highs::try_with_capacity(book.len()).unwrap();
        for ask in &book.asks {
            highs.insert_ask(ask);
        }
        for bid in &book.bids {
            highs.insert_bid(bid);
        }

        highs.match_orders(0, &mut fills);

        let (asks, bids): (Vec<_>, Vec<_>) =
            fills.into_iter().partition(|fill| fill.side == Side::Ask);
        let total_ask_qty: u64 = asks.into_iter().map(|ask| ask.qty.0.get()).sum();
        let total_bid_qty: u64 = bids.into_iter().map(|ask| ask.qty.0.get()).sum();

        if total_ask_qty == total_bid_qty {
            Ok(())
        } else {
            Err(format!(
                "conversation of flow violated: total ask quantity: {total_ask_qty} vs {total_bid_qty} total bid quantity"
            ))
        }
    }

    fn test_no_worse_off(book: &Book) -> Result<(), String> {
        let mut fills = vec![];
        let mut highs = Highs::try_with_capacity(book.len()).unwrap();
        for ask in &book.asks {
            highs.insert_ask(ask);
        }
        for bid in &book.bids {
            highs.insert_bid(bid);
        }

        highs.match_orders(0, &mut fills);

        let (filled_asks, filled_bids): (Vec<_>, Vec<_>) =
            fills.into_iter().partition(|fill| fill.side == Side::Ask);

        let all_ask_prices_ok = filled_asks
            .into_iter()
            .all(|fill| fill.price >= book.get_ask_by_id(fill.id).unwrap().price);
        let all_bid_prices_ok = filled_bids
            .into_iter()
            .all(|fill| fill.price <= book.get_bid_by_id(fill.id).unwrap().price);

        if all_ask_prices_ok && all_bid_prices_ok {
            Ok(())
        } else {
            Err(format!(
                "no worse off violated: all asks: {all_ask_prices_ok:?} vs {all_bid_prices_ok} all bids"
            ))
        }
    }

    fn test_cash_conservation(book: &Book) -> Result<(), String> {
        let mut fills = vec![];

        let mut highs = Highs::try_with_capacity(book.len()).unwrap();
        for ask in &book.asks {
            highs.insert_ask(ask);
        }
        for bid in &book.bids {
            highs.insert_bid(bid);
        }

        highs.match_orders(0, &mut fills);

        let (filled_asks, filled_bids): (Vec<_>, Vec<_>) =
            fills.into_iter().partition(|fill| fill.side == Side::Ask);
        let sum_asks: Decimal = filled_asks
            .into_iter()
            .map(|fill| fill.price.0 * Decimal::from_u64(fill.qty.0.get()).unwrap())
            .sum();
        let sum_bids = filled_bids
            .into_iter()
            .map(|fill| fill.price.0 * Decimal::from_u64(fill.qty.0.get()).unwrap())
            .sum();

        if sum_asks == sum_bids {
            Ok(())
        } else {
            Err(format!(
                "cash conservation violated: sum asks: {sum_asks} vs {sum_bids} sum bids"
            ))
        }
    }

    fn test_unique_clearing_price(book: &Book) -> Result<(), String> {
        let mut fills = vec![];

        let mut highs = Highs::try_with_capacity(book.len()).unwrap();
        for ask in &book.asks {
            highs.insert_ask(ask);
        }
        for bid in &book.bids {
            highs.insert_bid(bid);
        }

        highs.match_orders(0, &mut fills);

        let clearing_prices: HashSet<_> = fills.into_iter().map(|fill| fill.price.0).collect();
        if clearing_prices.len() <= 1 {
            Ok(())
        } else {
            Err(format!(
                "unique clearing price violated: clearing prices: {clearing_prices:?}"
            ))
        }
    }
}
