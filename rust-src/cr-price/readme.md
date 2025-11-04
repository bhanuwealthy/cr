# cr-price

A Rust command-line tool that calculates the best execution price for buying or selling Bitcoin across multiple exchanges (currently Coinbase and Gemini).

## Features

- Real-time price aggregation from multiple cryptocurrency exchanges
- Calculates optimal execution price for a given BTC quantity
- Rate limiting to respect exchange API limits
- Concurrent data fetching from multiple exchanges
- Efficient order book processing using Polars DataFrames

## Usage

```shell
# Get execution price for buying/selling 10 BTC (default)
cr-price

# Get execution price for specific quantity
cr-price --qty 5.5
```

## Installation

Ensure you have Rust installed, then:

```shell
cargo build --release
```

The binary will be available in `target/release/cr-price`

## Dependencies

- Polars - Fast DataFrame library
- Tokio - Async runtime
- Reqwest - HTTP client
- Clap - Command line argument parsing
- Serde - JSON serialization/deserialization
- Anyhow - Error handling

## Rate Limits

The tool respects exchange rate limits:
- Coinbase: 2 seconds between requests
- Gemini: 2 seconds between requests

## Example Output

```
To  buy 10 BTC = $512345.67
To sell 10 BTC = $512300.89
```

## Contributing

Feel free to submit issues and pull requests.

## License

This project is open source and available under the MIT License.