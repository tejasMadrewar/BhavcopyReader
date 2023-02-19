from pd_model import Symbol, Data, Mkt, Series, Security1
import pandas as pd
import sqlalchemy as db
import config as cfg
import matplotlib.pyplot as plt
import mplfinance as mpf


class DataManager:
    def __init__(self, session: db.orm.Session):
        self.session = session

    def setup_queries(self):
        query_get_symbol_info = ""

    def query_to_df(self, query):
        df = (pd.read_sql_query(query.statement, self.session.get_bind())
              .set_index("Date")
              )
        return df

    def get_equity_data(self, symbol):
        query = self.session.query(
            Data.date1.label("Date"),
            # Series.series_name.label("series"),
            Symbol.symbol_name.label("symbol"),
            Data.open_price.label("Open"),
            Data.high_price.label("High"),
            Data.low_price.label("Low"),
            Data.close_price.label("Close")
        ).join(
            Symbol, Series
            # Mkt
        ).filter(
            (Series.series_name == "EQ") |
            (Series.series_name == "BE")
        ) .filter(
            Symbol.symbol_name == symbol
        ).order_by(
            # db.desc(Data.date1)
            Data.date1
        )
        # print(query)
        df = self.query_to_df(query)
        return df

    def plot_equity(self, symbol):
        df = self.get_equity_data(symbol)
        print(df)
        print(df.dtypes)
        mpf.plot(df, type="candle", mav=(50, 200))


def main():
    session = db.orm.Session(cfg.SQL_CON)
    dMgr = DataManager(session)
    # dMgr.plot_equity("HINDUNILVR")
    dMgr.plot_equity("INFOSYSTCH")


if __name__ == "__main__":
    main()
