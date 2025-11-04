use anyhow::{Ok, Result};
use clap::Parser;
use polars::lazy::prelude::*;
use polars::prelude::*;
use polars_core::utils::concat_df;
use serde::{Deserialize, Serialize};
// use std::fs::File;
use std::sync::Arc;
use std::{f64, vec};
use tokio::sync::Mutex;
use tokio::task;
use tokio::time::{Duration, Instant};

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value_t = 10.0)]
    qty: f64,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    // let start_time = Instant::now();
    let cb_rl = RateLimiter::new(Duration::from_secs(2));
    let gem_rl = RateLimiter::new(Duration::from_secs(2));
    let (ex1, ex2) = tokio::join!(fetch_coinbase(&cb_rl), fetch_gemini(&gem_rl));
    // let proc_start = Instant::now();
    let (cb_bids, cb_asks) = cb_to_df(&ex1?)?;
    let (gem_bids, gem_asks) = gem_to_df(&ex2?)?;
    let (_all_bids, _all_asks) = tokio::join!(
        task::spawn_blocking(move || merge_dfs(cb_bids, gem_bids, "bids".to_string(), Some(true))),
        task::spawn_blocking(move || merge_dfs(cb_asks, gem_asks, "asks".to_string(), Some(false))),
    );
    // let _ = merge_dfs(cb_bids, gem_bids, "bids".to_string(), Some(true));
    // let _ = merge_dfs(cb_asks, gem_asks, "asks".to_string(), Some(false));

    // let proc_end = Instant::now();

    // println!(
    //     ">>> Proc took \t{:.3} ms",
    //     (proc_end - proc_start).as_millis()
    // );
    // println!(
    //     ">>> Main took \t{:.3} ms",
    //     (proc_end - start_time).as_millis()
    // );
    println!(
        "To  buy {:?} BTC = ${:?}",
        args.qty,
        calculate_execution_price(_all_asks??, args.qty).unwrap()
    );
    println!(
        "To sell {:?} BTC = ${:?}",
        args.qty,
        calculate_execution_price(_all_bids??, args.qty).unwrap()
    );

    Ok(())
}

#[derive(Clone)]
struct RateLimiter {
    last_call: Arc<Mutex<Instant>>,
    interval_duration: Duration,
}

impl RateLimiter {
    fn new(interval_duration: Duration) -> Self {
        Self {
            last_call: Arc::new(Mutex::new(Instant::now() - interval_duration)),
            interval_duration,
        }
    }

    async fn check_limit(&self) -> anyhow::Result<()> {
        let mut last = self.last_call.lock().await;
        let now = Instant::now();
        let next_allowed = *last + self.interval_duration;

        if next_allowed > now {
            // Since instructions were given not to wait , we are rising an Error
            let wait_sec = (next_allowed - now).as_secs_f64();
            return Err(anyhow::anyhow!(
                "Limit exceeded. Try after {:.2} sec",
                wait_sec
            ));
        }

        *last = Instant::now();
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CoinBaseData {
    pub bids: Vec<(String, String, u64)>,
    pub asks: Vec<(String, String, u64)>,
}

async fn fetch_coinbase(rate_limiter: &RateLimiter) -> anyhow::Result<CoinBaseData> {
    // let proc_start = Instant::now();
    rate_limiter.check_limit().await?;
    let url = "https://api.exchange.coinbase.com/products/BTC-USD/book?level=2";
    let resp_val: serde_json::Value = reqwest::get(url).await?.error_for_status()?.json().await?;
    // println!("fetch_coinbase complete");
    //if let Some(map) = resp_val.as_object() {
    //  println!("JSON Keys: {:?}", map.keys());
    //}
    let resp: CoinBaseData = serde_json::from_value(resp_val)?;
    // let proc_end = Instant::now();
    // println!(
    //     ">>> fetch_coinbase took \t{:.3} ms",
    //     (proc_end - proc_start).as_millis()
    // );
    Ok(resp)
}

fn cb_to_df(cb: &CoinBaseData) -> Result<(DataFrame, DataFrame)> {
    // let start = Instant::now();
    let mut prices: Vec<f64> = Vec::new();
    let mut qtys: Vec<f64> = Vec::new();

    for (price, qty, _) in cb.bids.iter() {
        prices.push(price.parse::<f64>()?);
        qtys.push(qty.parse::<f64>()?);
    }

    let cb_bids = DataFrame::new(vec![
        Series::new("price".into(), &prices).into(),
        Series::new("qty".into(), &qtys).into(),
    ])?;
    // println!("cb_bids shape={:?}", &cb_bids.shape());

    prices.clear();
    qtys.clear();

    for (price, qty, _) in cb.asks.iter() {
        prices.push(price.parse::<f64>()?);
        qtys.push(qty.parse::<f64>()?);
    }
    let cb_asks = DataFrame::new(vec![
        Series::new("price".into(), &prices).into(),
        Series::new("qty".into(), &qtys).into(),
    ])?;
    // println!("cb_asks shape={:?}", &cb_asks.shape());
    prices.clear();
    qtys.clear();
    // println!(
    //     ">>> cb_to_df took \t{}ms",
    //     (Instant::now() - start).as_millis()
    // );
    // let cb_bids_file = File::create("bids_cb.csv")?;
    // let cb_asks_file = File::create("aks_cb.csv")?;
    // let _ = CsvWriter::new(cb_bids_file).finish(&mut cb_bids)?;
    // let _ = CsvWriter::new(cb_asks_file).finish(&mut cb_asks)?;

    Ok((cb_bids, cb_asks))
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GemOrder {
    pub price: String,
    pub amount: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GeminData {
    pub bids: Vec<GemOrder>,
    pub asks: Vec<GemOrder>,
}

async fn fetch_gemini(rate_limiter: &RateLimiter) -> anyhow::Result<GeminData> {
    // let start = Instant::now();

    rate_limiter.check_limit().await?;
    let url = "https://api.gemini.com/v1/book/BTCUSD";
    let resp_val: serde_json::Value = reqwest::get(url)
        .await?
        .error_for_status()?
        .error_for_status()?
        .json()
        .await?;
    // println!("fetch_gemini complete");
    // if let Some(map) = resp_val.as_object() {
    //     println!("JSON Keys: {:?}", map.keys());
    // }
    let resp: GeminData = serde_json::from_value(resp_val)?;
    // println!(
    //     ">>> fetch_gemini took \t{}ms",
    //     (Instant::now() - start).as_millis()
    // );
    Ok(resp)
}

fn gem_to_df(gem: &GeminData) -> anyhow::Result<(DataFrame, DataFrame)> {
    // let start = Instant::now();

    let mut prices: Vec<f64> = Vec::new();
    let mut qts: Vec<f64> = Vec::new();

    for row in gem.bids.iter() {
        prices.push(row.price.parse::<f64>()?);
        qts.push(row.amount.parse::<f64>()?)
    }
    let gem_bids = DataFrame::new(vec![
        Series::new("price".into(), &prices).into(),
        Series::new("qty".into(), &qts).into(),
    ])?;
    // println!("gem_bids shape: {:?}", &gem_bids.shape());
    prices.clear();
    qts.clear();

    for row in gem.asks.iter() {
        prices.push(row.price.parse::<f64>()?);
        qts.push(row.amount.parse::<f64>()?);
    }

    let gem_asks = DataFrame::new(vec![
        Series::new("price".into(), &prices).into(),
        Series::new("qty".into(), &qts).into(),
    ])?;

    // println!("gem_asks shape: {:?}", &gem_asks.shape());
    // println!(
    //     ">>> gem_to_df took \t{}ms",
    //     (Instant::now() - start).as_millis()
    // );
    // let gem_bids_file = File::create("bids_gem.csv")?;
    // let gem_asks_file = File::create("asks_gem.csv")?;
    // let _ = CsvWriter::new(gem_bids_file).finish(&mut gem_bids);
    // let _ = CsvWriter::new(gem_asks_file).finish(&mut gem_asks);
    Ok((gem_bids, gem_asks))
}

fn merge_dfs(
    cb_df: DataFrame,
    gem_df: DataFrame,
    _type: String,
    desc: Option<bool>,
) -> anyhow::Result<DataFrame> {
    // let start = Instant::now();
    // println!("starting.. to merge {}", &_type);
    let mut merged_df = concat_df(&[gem_df, cb_df])?;
    let desc_type = desc.unwrap_or(false);
    let cols: Vec<String> = merged_df
        .get_column_names()
        .iter()
        .map(|s| s.to_string())
        .collect();
    merged_df = merged_df.drop_nulls(Some(&cols))?;

    merged_df = merged_df
        .lazy()
        .sort(
            ["price"],
            SortMultipleOptions::default().with_order_descending(desc_type),
        )
        .with_columns(vec![col("qty").cum_sum(false).alias("cum_sum_qty")])
        .collect()?;

    // println!(
    //     "{}-merge desc={} df shape{:#?} ",
    //     _type,
    //     desc_type,
    //     &merged_df.head(Some(3))
    // );
    // println!(
    //     ">>> merge_dfs took \t{}ms",
    //     (Instant::now() - start).as_millis()
    // );

    // let my_file = File::create(format!("{}.csv", _type))?;
    // let _ = CsvWriter::new(my_file).finish(&mut merged_df);
    Ok(merged_df)
}

fn calculate_execution_price(orders: DataFrame, target_qty: f64) -> anyhow::Result<f64> {
    let result_df = orders
        .lazy()
        .with_columns([
            (col("qty").cum_sum(false).shift(lit(1)).fill_null(lit(0.0))).alias("cum_qty_before"),
        ])
        .with_columns([(lit(target_qty) - col("cum_qty_before"))
            .clip_min(lit(0.0))
            .alias("remaining_to_fill")])
        .with_column(
            when(col("qty").lt(col("remaining_to_fill")))
                .then(col("qty"))
                .otherwise(col("remaining_to_fill"))
                .alias("filled_qty_at_level"),
        )
        .with_columns([(col("filled_qty_at_level") * col("price")).alias("cost_at_level")])
        .collect()?;

    // let result_file = File::create("result.csv")?;
    // let _ = CsvWriter::new(result_file).finish(&mut result_df)?;
    let total_cost = result_df
        .column("cost_at_level")?
        .f64()?
        .sum()
        .unwrap_or(0.0);

    Ok(total_cost)
}
