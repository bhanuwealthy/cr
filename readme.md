### This script is built with `python[typer]`

## Install requirements
```shell
# ultra fast, expects uv to be installed
$ brew install uv 
$ uv pip install -r reqs.txt

# slow, old fashioned
$ pip install -r reqs.txt
```

## Usage
```shell
$ python main.py --help

Usage: main.py [OPTIONS] COMMAND [ARGS]...   
╭─ Commands ────
│ calculate-fast           
│ calculate        
╰────────────

$ python main.py calculate --help

Usage: main.py calculate [OPTIONS]
╭─ Options
│ --qty                  FLOAT  [default: 10.0]                                                                                     │
│ --fast    --no-fast           [default: no-fast]
│ --help
╰────────

$ python main.py calculate --qty=1e-2 
fetching coinbase data..
To buy  0.01 BTC= $ 1144.2003
To sell 0.01 BTC= $ 1144.1907830300001

$ python main.py calculate --qty=1e-2 --fast
Using dataframe..
fetching coinbase data..
fetching gemini data..
To buy  0.01 BTC= $ 1143.0
To sell 0.01 BTC= $ 1143.1081
```

