import pandas as pd
import sqlalchemy as db
from sqlalchemy.sql import exists
import mplfinance as mpf

from pd_model import Symbol, Data, Mkt, Series, Security1, CorpAction
import config as cfg
import nameChangeModel as nameChange


class DataManager:
    def __init__(self, session: db.orm.Session):
        self.session = session
        self.nameChange = nameChange.NameChangeManager(session.get_bind())

    def setup_queries(self):
        query_get_symbol_info = ""

    def query_to_df(self, query):
        df = pd.read_sql_query(query.statement, self.session.get_bind()).set_index(
            "Date"
        )
        return df

    def generate_symbol_id_filter(self, symbol_name: str, table):
        ids = self.nameChange.get_ids_of_symbol(symbol_name)
        return db.or_(table.symbol_id == i for i in ids)

    def get_equity_data(self, symbol: str, adjusted=True):
        query = (
            self.session.query(
                Data.date1.label("Date"),
                # Series.series_name.label("series"),
                Symbol.symbol_name.label("symbol"),
                Data.open_price.label("Open"),
                Data.high_price.label("High"),
                Data.low_price.label("Low"),
                Data.close_price.label("Close"),
            )
            .join(
                Symbol,
                Series
                # Mkt
            )
            .filter((Series.series_name == "EQ") | (Series.series_name == "BE"))
            .filter(self.generate_symbol_id_filter(symbol.upper(), Data))
            .order_by(
                # db.desc(Data.date1)
                Data.date1
            )
        )
        df = self.query_to_df(query)
        return df

    def get_corpAction_data(self, symbol: str, adjusted=True):
        query = (
            self.session.query(
                Symbol.symbol_name.label("symbol"),
                CorpAction.date1.label("date1"),
                CorpAction.record_dt.label("record_dt"),
                CorpAction.ex_dt.label("ex_dt"),
                CorpAction.purpose.label("purpose"),
            )
            .join(
                Symbol,
            )
            .filter(self.generate_symbol_id_filter(symbol.upper(), CorpAction))
            .order_by(CorpAction.date1, CorpAction.record_dt, CorpAction.ex_dt)
            .distinct()
        )
        # print(query.statement)
        df = pd.read_sql_query(query.statement, self.session.get_bind())
        df.drop_duplicates(["symbol", "ex_dt", "purpose"], inplace=True)
        df.reset_index(drop=True, inplace=True)
        return df

    def get_all_tickers(self):
        query = (
            self.session.query(Symbol.symbol_name.label("symbol"))
            .order_by(Symbol.symbol_name)
            .distinct()
        )
        df = pd.read_sql_query(query.statement, self.session.get_bind())
        return df

    def plot_equity(self, symbol: str, ax=None):
        if self.is_ticker_valid(symbol):
            df = self.get_equity_data(symbol)
            # print(df.dtypes)
            if ax == None:
                mpf.plot(df, type="candle", mav=(50, 200))
            else:
                mpf.plot(df, type="candle", mav=(50, 200), ax=ax)

    def is_ticker_valid(self, ticker: str):
        a = self.session.query(
            exists().where(Symbol.symbol_name == ticker.upper())
        ).scalar()
        return a


def test_get_corp_action():
    session = db.orm.Session(cfg.SQL_CON)
    dMgr = DataManager(session)
    df = dMgr.get_corpAction_data("TCS")
    print(df)
    df.to_csv("corp_data_test.csv")


def test_is_symbol_valid():
    session = db.orm.Session(cfg.SQL_CON)
    dMgr = DataManager(session)
    tickers = ["asdf", "Tcs", "TCS", "fffff", "HDFC"]
    for t in tickers:
        print(t, dMgr.is_ticker_valid(t))


def main():
    session = db.orm.Session(cfg.SQL_CON)
    dMgr = DataManager(session)
    # dMgr.plot_equity("HINDUNILVR")
    # dMgr.plot_equity("YAARII")
    dMgr.plot_equity("INFY")
    print(dMgr.get_all_tickers())


if __name__ == "__main__":
    # main()
    # test_get_corp_action()
    test_is_symbol_valid()
