#[cfg(test)]
pub mod tests {
    use good_lp::variable;
    use good_lp::{
        Constraint, DualValues, Expression, Solution, SolutionWithDual, SolverModel, Variable,
        constraint, default_solver, solvers::highs::HighsSolution, variable::UnsolvedProblem,
        variables,
    };
    use rust_decimal::Decimal;
    use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
    use tracing::{debug, error, info, instrument, warn};

    use crate::engine::model::tests::Book;
    use crate::engine::model::{Fill, Price, Qty, Side};

    type Idx = usize;

    struct Buyers(Vec<(Variable, Idx)>);
    struct Sellers(Vec<(Variable, Idx)>);

    #[instrument(skip(book))]
    pub fn match_orders(auction_id: u64, book: &Book) -> Vec<Fill> {
        info!(
            id = auction_id,
            num_orders = book.len(),
            "Starting auction match"
        );

        if book.asks.is_empty() || book.bids.is_empty() {
            return vec![];
        }

        let (market_clearing_constraint, problem, buyers, sellers) =
            construct_constraint_and_problem(book);

        let mut model = default_solver(problem)
            .set_threads(1) // Force single-threaded for deterministic replays
            .set_option("random_seed", 42); // Fix seed for determinism
        let market_clearing = model.add_constraint(market_clearing_constraint);

        let mut solution = match model.solve() {
            Ok(solution) => solution,
            Err(e) => {
                error!("Couldn't solve model: {e}");
                return vec![];
            }
        };

        let clearing_price = if marginal_trader_exist(&buyers, &sellers, &solution, book) {
            let shadow_price = solution.compute_dual().dual(market_clearing);

            Decimal::from_f64(shadow_price)
        } else {
            let p = calculate_midpoint_price(&buyers, &sellers, &solution, book);
            debug!("naive mid point price {p:?}");

            p
        };

        let clearing_price = match clearing_price {
            Some(price) => price,
            // Should only happen when noone gets filled
            None => {
                // too noisy?
                info!("No matches in auction");
                return vec![];
            }
        };

        create_fills(&buyers, &sellers, &solution, &clearing_price, book)
    }

    fn create_fills(
        buyers: &Buyers,
        sellers: &Sellers,
        solution: &HighsSolution,
        clearing_price: &Decimal,
        book: &Book,
    ) -> Vec<Fill> {
        let seller_fills = sellers
            .0
            .iter()
            .filter(|(seller, _)| solution.value(*seller) > 0.0)
            .map(|(seller, idx)| {
                (
                    Qty::new(
                        NonZeroU64::new(solution.eval(seller) as u64)
                            .expect("Every non-zero quantity is a valid u64"),
                    ),
                    *idx,
                )
            })
            .map(|(qty, idx)| {
                let order = &book.asks[idx];
                Fill::new(order.id, qty, Price::new(*clearing_price), Side::Ask)
            });

        let buyer_fills = buyers
            .0
            .iter()
            .filter(|(seller, _)| solution.value(*seller) > 0.0)
            .map(|(seller, idx)| {
                (
                    Qty::new(
                        NonZeroU64::new(solution.eval(seller) as u64)
                            .expect("Every non-zero quantity is a valid u64"),
                    ),
                    *idx,
                )
            })
            .map(|(qty, idx)| {
                let order = &book.bids[idx];
                Fill::new(order.id, qty, Price::new(*clearing_price), Side::Bid)
            });

        seller_fills.chain(buyer_fills).collect()
    }

    fn calculate_midpoint_price(
        buyers: &Buyers,
        sellers: &Sellers,
        solution: &HighsSolution,
        book: &Book,
    ) -> Option<Decimal> {
        let highest_filled_seller = sellers
            .0
            .iter()
            .filter(|(seller, _)| solution.value(*seller) > 0.0)
            .map(|(_, idx)| book.asks[*idx].price.0)
            .max_by(|a, b| a.cmp(b))?;

        let lowest_filled_buyer = buyers
            .0
            .iter()
            .filter(|(buyer, _)| solution.value(*buyer) > 0.0)
            .map(|(_, idx)| book.bids[*idx].price.0)
            .min_by(|a, b| a.cmp(b))?;

        Some((highest_filled_seller + lowest_filled_buyer) / Decimal::TWO)
    }

    fn marginal_trader_exist(
        buyers: &Buyers,
        sellers: &Sellers,
        solution: &HighsSolution,
        book: &Book,
    ) -> bool {
        buyers.0.iter().any(|(buyer, orders_idx)| {
            let filled = solution.value(*buyer);
            let qty = book.bids[*orders_idx].qty.as_f64();
            let id = book.bids[*orders_idx].id;
            debug!("bids naive filled {} {} qty of id {:?}", filled, qty, id);

            filled > 0.0 && filled < qty
        }) || sellers.0.iter().any(|(seller, orders_idx)| {
            let filled = solution.value(*seller);
            let qty = book.asks[*orders_idx].qty.as_f64();
            let id = book.asks[*orders_idx].id;
            debug!("asks naive filled {} {} qty of id {:?}", filled, qty, id);

            filled > 0.0 && filled < qty
        })
    }

    // TODO: Cannot reuse any of these. Maybe good_lp is not the best layer.
    #[instrument(skip_all)]
    fn construct_constraint_and_problem(
        book: &Book,
    ) -> (Constraint, UnsolvedProblem, Buyers, Sellers) {
        let mut vars = variables!();
        let mut welfare = Expression::with_capacity(book.len());
        let mut bought_sub_sold = Expression::with_capacity(book.len());

        let mut buyers = Vec::with_capacity(book.len());
        let mut sellers = Vec::with_capacity(book.len());

        for (idx, order) in book.bids.iter().enumerate() {
            let v = vars.add(variable().min(0).max(order.qty.as_f64()));
            let price = order
                .price
                .0
                .to_f64()
                .expect("tick representation always valid f64");

            buyers.push((v, idx));
            welfare += v * price;
            bought_sub_sold += v;
        }

        for (idx, order) in book.asks.iter().enumerate() {
            let v = vars.add(variable().min(0).max(order.qty.as_f64()));
            let price = order
                .price
                .0
                .to_f64()
                .expect("tick representation always valid f64");

            sellers.push((v, idx));
            welfare -= v * price;
            bought_sub_sold -= v;
        }

        let problem = vars.maximise(welfare);
        let market_clearing = constraint!(bought_sub_sold == 0);

        (market_clearing, problem, Buyers(buyers), Sellers(sellers))
    }

    use std::{
        collections::{BTreeSet, HashSet},
        num::NonZeroU64,
        time::Instant,
    };

    use proptest::prelude::*;
    use rand::{RngExt, SeedableRng, rngs::SmallRng};
    use rust_decimal::dec;

    use crate::engine::model::tests::book_strategy;
    use crate::engine::model::{Id, Order, Seq};

    #[test]
    fn empty_no_matches() {
        let book = Book::with_capacity(0);

        let fills = match_orders(0, &book);

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

        let mut book = Book::with_capacity(2);
        book.insert_bid(bid);
        book.insert_ask(ask);

        let fills = match_orders(0, &book);

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

        let mut book = Book::with_capacity(2);
        book.insert_bid(bid.clone());
        book.insert_ask(ask.clone());

        let fills = match_orders(0, &book);

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

        let mut book = Book::with_capacity(3);
        book.insert_bid(bid1.clone());
        book.insert_bid(bid2.clone());
        book.insert_ask(ask.clone());

        let fills = match_orders(0, &book);

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

        let mut book = Book::with_capacity(3);
        book.insert_bid(bid1.clone());
        book.insert_bid(bid2.clone());
        book.insert_ask(ask.clone());

        let fills = match_orders(0, &book);

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

        let mut book = Book::with_capacity(3);
        book.insert_bid(bid1.clone());
        book.insert_bid(bid2.clone());
        book.insert_ask(ask.clone());

        let fills = match_orders(0, &book);

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

        let mut book = Book::with_capacity(3);
        book.insert_bid(bid1.clone());
        book.insert_bid(bid2.clone());
        book.insert_ask(ask.clone());

        let fills = match_orders(0, &book);
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

        let mut book = Book::with_capacity(4);
        book.insert_bid(bid1.clone());
        book.insert_bid(bid2.clone());
        book.insert_ask(ask1.clone());
        book.insert_ask(ask2.clone());

        let fills = match_orders(0, &book);

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
    }

    fn test_conservation_of_flow(book: &Book) -> Result<(), String> {
        let fills = match_orders(0, &book);

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
        let fills = match_orders(0, &book);

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
                "no worse off violated: all asks: {all_ask_prices_ok} vs {all_bid_prices_ok} all bids"
            ))
        }
    }

    fn test_cash_conservation(book: &Book) -> Result<(), String> {
        let fills = match_orders(0, &book);

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
        let fills = match_orders(0, &book);

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
