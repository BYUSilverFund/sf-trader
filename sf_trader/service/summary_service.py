import polars as pl
from sf_trader.config import Config
from rich.console import Console

from sf_trader.dal.dao.portfolio_dao import PortfolioDAO
from sf_trader.service.ui_service import UIService
from sf_trader.service.calculate_service import CalculateService
from sf_trader.dal.models.schema_models import (
    SharesDF, OrdersDF, PricesDF, SharesSchema, 
)


class SummaryService:
    def __init__(
        self,
        config: Config,
        portfolio_dao: PortfolioDAO | None = None,
        calculate_service: CalculateService | None = None,
    ):
        self.portfolio_dao = portfolio_dao or PortfolioDAO()
        self.calculate_service = calculate_service or CalculateService(config)
        self.ui_service = UIService()
        self.config = config
        self.broker = config.broker


    def get_portfolio_summary(self, shares: SharesDF) -> None:
        # Get account value
        account_value = self.broker.get_account_value()

        # Get tickers
        tickers = shares["ticker"].to_list()

        # Get prices
        prices = self.portfolio_dao.get_prices_by_date(date=self.config.data_date, tickers=tickers)

        # Get dollars
        dollars = self.calculate_service.get_dollars(shares=shares, prices=prices)
        dollars_allocated = dollars["dollars"].sum()

        # Calculate portfolio weights from dollars
        weights = self.calculate_service.get_weights_from_dollars(
            dollars=dollars, account_value=account_value
        )

        # Get benchmark weights
        benchmark = self.portfolio_dao.get_benchmark_weights_by_date(date=self.config.data_date)

        # Get universe
        universe = benchmark["ticker"].sort().to_list()

        # Get covariance matrix
        covariance_matrix = self.calculate_service.get_covariance_matrix(tickers=universe)

        # Decompose weights
        total_weights, active_weights = self.calculate_service.decompose_weights(
            benchmark=benchmark, weights=weights
        )

        # Generate portfolio metrics table
        portfolio_metrics = self.calculate_service.get_portfolio_metrics(
            total_weights=total_weights,
            active_weights=active_weights,
            covariance_matrix=covariance_matrix,
            account_value=account_value,
            dollars_allocated=dollars_allocated,
        )
        portfolio_metrics_table = self.ui_service.generate_portfolio_metrics_table(
            portfolio_metrics
        )

        # Generate top long positions table
        top_long_positions = self.calculate_service.get_top_long_positions(
            shares=shares,
            prices=prices,
            dollars=dollars,
            weights=weights,
            benchmark=benchmark,
            account_value=account_value,
        )
        top_long_positions_table = self.ui_service.generate_positions_table(
            positions=top_long_positions, title="Top 10 Long Positions"
        )

        # Render UI
        console = Console()
        console.print()
        console.print(portfolio_metrics_table)
        console.print()
        console.print(top_long_positions_table)


    def get_orders_summary(
        self, shares: SharesDF, orders: OrdersDF
    ) -> None:
        """
        Generate and display orders summary tables.

        Args:
            shares: DataFrame with ticker and optimal shares columns
            orders: DataFrame with ticker, price, shares, action columns
            config: Configuration object
        """
        current_shares = self.broker.get_positions()

        #connect to Database
        port_dao = PortfolioDAO()

        # Compute ticker list from both current and optimal portfolios
        tickers = list(set(current_shares["ticker"].to_list() + shares["ticker"].to_list()))

        # Get prices for all tickers
        prices = port_dao.get_prices_by_date(date=self.config.data_date, tickers=tickers)

        # Create combined shares dataframe with both current and optimal shares
        combined_shares = self.get_combined_shares(
            current_shares=current_shares, optimal_shares=shares
        )

        # Get top 10 long positions from current shares
        top_long_orders = self.get_top_long_orders(
            shares=combined_shares, prices=prices, orders=orders, top_n=10
        )

        top_long_orders_table = self.ui_service.generate_orders_table(
            orders=top_long_orders, title="Top 10 Long Position Orders"
        )

        # Get top 10 active BUY orders by dollar value
        top_active_buy_orders = self.get_top_active_orders(
            shares=combined_shares, orders=orders, prices=prices, action="BUY", top_n=10
        )
        top_active_buy_orders_table = self.ui_service.generate_orders_table(
            orders=top_active_buy_orders, title="Top 10 Active BUY Orders by Dollar Value"
        )

        # Get top 10 active SELL orders by dollar value
        top_active_sell_orders = self.get_top_active_orders(
            shares=combined_shares, orders=orders, prices=prices, action="SELL", top_n=10
        )
        top_active_sell_orders_table = self.ui_service.generate_orders_table(
            orders=top_active_sell_orders, title="Top 10 Active SELL Orders by Dollar Value"
        )

        # Render UI
        console = Console()
        console.print()
        console.print(top_long_orders_table)
        console.print()
        console.print(top_active_buy_orders_table)
        console.print()
        console.print(top_active_sell_orders_table)


    @staticmethod
    def get_top_long_orders(
        shares: SharesDF,
        prices: PricesDF,
        orders: OrdersDF,
        top_n: int = 10,
    ) -> pl.DataFrame:
        long_positions = (
            shares.join(prices, on="ticker", how="left")
            .join(
                orders.select("ticker", pl.col("shares").alias("to_trade"), "action"),
                on="ticker",
                how="left",
            )
            .with_columns(
                pl.col("action").fill_null("HOLD"),
                pl.col("to_trade").fill_null(0),
                pl.when(pl.col("price").is_null())
                .then(pl.lit(9999))
                .otherwise(pl.col("price"))
                .alias("price"),
            )
            .with_columns(
                (pl.col("shares") * pl.col("price")).alias("dollars"),
            )
            .filter(pl.col("shares") > 0)  # Only long positions
            .sort("dollars", descending=True)
            .head(top_n)
            .select("ticker", "shares", "price", "dollars", "to_trade", "action")
        )

        return long_positions


    @staticmethod
    def get_top_active_orders(
        shares: SharesDF,
        orders: OrdersDF,
        prices: PricesDF,
        action: str,
        top_n: int = 10,
    ) -> pl.DataFrame:
        active_orders = (
            shares.join(prices, on="ticker", how="left")
            .join(
                orders.select("ticker", pl.col("shares").alias("to_trade"), "action"),
                on="ticker",
                how="left",
            )
            .with_columns(
                pl.col("action").fill_null("HOLD"),
                pl.col("to_trade").fill_null(0),
                pl.when(pl.col("price").is_null())
                .then(pl.lit(9999))
                .otherwise(pl.col("price"))
                .alias("price"),
            )
            .with_columns(
                (pl.col("shares") * pl.col("price")).alias("dollars"),
            )
            .filter(
                pl.col("action").eq(action),  # Filter by specific action (BUY or SELL)
            )
            .sort("dollars", descending=True)
            .head(top_n)
            .select("ticker", "shares", "price", "dollars", "to_trade", "action")
        )

        return active_orders


    def get_combined_shares(
        self,
        current_shares: SharesDF,
        optimal_shares: SharesDF,
    ) -> SharesDF:
        # Get all unique tickers from both dataframes
        all_tickers = list(
            set(current_shares["ticker"].to_list() + optimal_shares["ticker"].to_list())
            - set(self.config.ignore_tickers)
        )

        # Create a dataframe with all tickers
        all_tickers_df = pl.DataFrame({"ticker": all_tickers})

        # Join with current shares to get actual holdings
        combined = all_tickers_df.join(
            current_shares, on="ticker", how="left"
        ).with_columns(
            pl.col("shares").fill_null(0),
        )

        return SharesSchema.validate(combined)
    