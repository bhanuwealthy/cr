import httpx
import pandas as pd
import typer
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time

app = typer.Typer()
MAX_WORKERS = 2


class Ratelimiter:
    def __init__(self, wait_sec: int = 2):
        self.limit_seconds = wait_sec
        self.tracker = {}
        # self.cache = {}
        self._lock = Lock()

    def __call__(self, func):
        def _wrapper(api, result, key ):
            current_time = time.time()
            with self._lock:
                last_call = self.tracker.get(key, 0)
                time_diff = current_time - last_call
                if time_diff > self.limit_seconds:
                    self.tracker[key] = current_time
                    resp = func(api, result, key)
                    # self.cache[key] = resp
                    return resp
                else:
                    print(key, 'fetch skipped')
                    raise Exception('Exceed limit')

        _wrapper.__name__ = f'rl_{func.__name__}'
        return _wrapper


exchanges = {
    'cb': 'https://api.exchange.coinbase.com/products/BTC-USD/book?level=2',
    'gem':  'https://api.gemini.com/v1/book/BTCUSD'
}

@Ratelimiter(wait_sec=2)
def _fetch_orders(api, result: dict = None, key: str = None) -> dict:
    print('fetching exchange data..', api[:35])
    resp = httpx.get(api)
    print(key, 'fetch complete')
    resp.raise_for_status()
    if key and type(result) == dict:
        result[key] = resp.json()
        return result
    return resp.json()


def calculate_price(orders: list, target_qty: float = 10):  # specific to coinbase
    remaining_qty: float = target_qty
    order_value: float = 0.0
    for order in orders:
        if remaining_qty <= 0:
            break
        order_price, order_qty = float(order[0]), float(order[1])
        available_qty = min(order_qty, remaining_qty)
        order_value += available_qty * order_price
        remaining_qty -= available_qty
        # print([order_price, order_qty, available_qty, remaining_qty])
    if remaining_qty > 0:
        print(f'\tPartial order:\n\tfilled={target_qty - remaining_qty};\n\t{remaining_qty=}')

    return order_value


DEFAULT_PRICE = 0.0


def calculate_execution_price_df(orders: pd.DataFrame, target_qt: float = 10) -> float:
    if orders.empty:
        return DEFAULT_PRICE
    insert_position = orders['cum_sum_qty'].searchsorted(target_qt)
    # print(f">> {insert_position=}")
    # print(orders[insert_position - 1: insert_position + 1])
    if insert_position == 0:  # means lesser quantity than order book
        # print(orders.iloc[0], 'target=', target_qt)
        return target_qt * orders.iloc[0]['price']

    if insert_position >= len(orders) - 1:  # more than order book quantity, leads to partial fill the target_qty
        print(f"Partial fill --{target_qt=}; --remaining_qty={target_qt - orders.iloc[-1]['cum_sum_qty']}")
        return orders.iloc[-1]['cum_sum_execution_price']

    # somewhere in between the head and tail
    # print(orders[insert_position-1: insert_position+1])
    for idx, row in orders[insert_position - 1: insert_position + 1].iterrows():
        # find 2 exact rows where target_qty would fit
        # print(f'processing row_idx={idx}')
        # print('\n\t---', row['cum_sum_qty'])
        if row['cum_sum_qty'] == target_qt:
            # print('------- found exact quantity')
            return row['cum_sum_execution_price']
        if row['cum_sum_qty'] < target_qt < orders.iloc[idx + 1]['cum_sum_qty']:
            # print(f'Found exact position: {(idx, idx + 1)}')
            return row['cum_sum_execution_price'] + (
                    ( target_qt - orders.iloc[idx]['cum_sum_qty']) * orders.iloc[idx + 1]['price'])
    return DEFAULT_PRICE


def fetch_all_data():
    result = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_exchange = {
            executor.submit(_fetch_orders, url, result, key): key
            for key, url in exchanges.items()
        }

        for future in as_completed(future_to_exchange):
            exchange_name = future_to_exchange[future]

            try:
                future.result()
                print(f"Finished processing and saving data for: {exchange_name}")
            except Exception as exc:
                print(f"Task for {exchange_name} ended with a critical exception: {exc}")
    return result


@app.command()
def calculate_fast(qty: float = 10.0):
    print('Using dataframe..')
    all_data = fetch_all_data()
    start = time.time()
    cb_data = all_data['cb']
    gem_data = all_data['gem']
    # *** Start CB
    cols = ['price', 'qty', 'ods']
    # CB: asks
    cb_asks = pd.DataFrame(cb_data['asks'], columns=cols)
    del cb_asks['ods']
    cb_asks[cols[:2]] = cb_asks[cols[:2]].astype(float)

    # CB: bids
    cb_bids = pd.DataFrame(cb_data['bids'], columns=cols)
    del cb_bids['ods']
    cb_bids[cols[:2]] = cb_bids[cols[:2]].astype(float)
    # *** CB End ***
    # ---------------------------------
    # *** Gemini: start
    gem_bids, gem_asks = pd.DataFrame(gem_data['bids']), pd.DataFrame(gem_data['asks'])
    del gem_bids['timestamp']
    del gem_asks['timestamp']

    gem_cols = ['price', 'qty']
    gem_bids.rename(columns={'amount': 'qty'}, inplace=True)
    gem_bids[gem_cols] = gem_bids[gem_cols].astype(float)

    gem_asks.rename(columns={'amount': 'qty'}, inplace=True)
    gem_asks[gem_cols] = gem_asks[gem_cols].astype(float)

    # ** Gem: Ends
    # ---------------------------------
    # Merge both bids
    merged_bids = pd.concat((cb_bids, gem_bids)).dropna().groupby('price').sum().reset_index()
    merged_bids = merged_bids.sort_values(by='price', ascending=False, ignore_index=True)
    # Merge both asks
    merged_asks = pd.concat((cb_asks, gem_asks)).dropna().groupby('price').sum().reset_index()
    merged_asks = merged_asks.sort_values(by='price', ascending=True, ignore_index=True)

    # Calculate cumulative
    merged_bids['cum_sum_qty'] = merged_bids['qty'].cumsum()
    merged_bids['cum_sum_execution_price'] = (merged_bids['price'] * merged_bids['qty']).cumsum()
    # ---
    merged_asks['cum_sum_qty'] = merged_asks['qty'].cumsum()
    merged_asks['cum_sum_execution_price'] = (merged_asks['price'] * merged_asks['qty']).cumsum()
    print(f'Data preparation time: {1000*(time.time() - start):.2f} ms')

    # ---------------------------------
    print(f'To buy  {qty} BTC= $', calculate_execution_price_df(merged_asks, target_qt=qty))
    # print('-' * 12)
    print(f'To sell {qty} BTC= $', calculate_execution_price_df(merged_bids, target_qt=qty))


@app.command()
def calculate(qty: float = 10.0, fast: bool = False):
    if fast:
        calculate_fast(qty)
        # test code for simulating rate limits
        # time.sleep(2)
        # calculate_fast(qty)
    else:
        cb_data = _fetch_orders(exchanges['cb'])
        print(f'To buy  {qty} BTC= $', calculate_price(cb_data['asks'], target_qty=qty))
        # print('-' * 12)
        print(f'To sell {qty} BTC= $', calculate_price(cb_data['bids'], target_qty=qty))


if __name__ == '__main__':
    app()
