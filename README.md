# Silver Fund Trading Repository

## Setup
1. Initialize Python virtual environment

```bash
uv sync
```

2. Download [IBKR Trader Work Station (TWS)](https://www.interactivebrokers.com/en/trading/download-tws.php?p=latest)
- Make sure to download the "latest" version for synchronous API support

3. Download [IBKR TWS API](https://interactivebrokers.github.io/#)
- Make sure to download the "latest" version for synchronous API support
- Instructions for how to unpackage the download is [here](https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/#unix-install)
- Hint: you don't need to use "sudo" on the super computer

4. Move `IBJts` folder to `sf-trader` repository root

5. Run `setup.py` file for `IBJts`
- Run the following commands from the root of the `sf-trader` repository

```bash
cd IBJts/source/pythonclient
python setup.py install
```

5. Test
- Run the following from the root of the `sf-trader` repository
- Note: you might need to run it a few times to make sure it works

```bash
python sf_trader get-account-value
```

## Trading
Note that all configuration for trading is in the `config.yml` file. This includes a parameter called `data-date` which should be set to the most recently completed trading day (usually yesterday) for live trading.

1. Ensure all data is downloaded:
- To update universe mapping run the following from `sf-data-pipelines-quant`:

```bash
python pipelines ftse backfill --database production
```

- To backfill barra data run the following from `sf-data-pipelines-quant`:
- Note that this flow runs every day and should already be up to date.

```bash
python pipelines barra backfill --database production
python piplines barra update --pdatabase roduction
```

2. Generate portfolios

```bash
python sf_trader get-portfolio
```

3. Generate trade list (orders)

```bash
python sf_trader get-orders
```

4. Check portfolio and orders
- At this point if the active risk isn't 5% you should adjust the gamma and repeat steps 2-4 until active risk is about 5%.

```bash
python sf_trader get-portfolio-summary
python sf_trader get-orders-summary
```

5. Place orders

```bash
python sf_trader post-orders
```

6. Cancel orders
- If TWS crashes while placing orders run the following to cancel outstanding orders and then repeat step 5. Rinse and repeat until there are no more orders to place.

```bash
python sf_trader cancel-orders
```