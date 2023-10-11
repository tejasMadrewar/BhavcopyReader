import pandas as pd
import sqlalchemy as db
from sqlalchemy.sql import exists
import mplfinance as mpf
from datetime import date, datetime

from model import Symbol, Data, Series
from config import SQL_CON, DOWNLOAD_FOLDER

# import nameChangeModel as nameChange
# from BseCorpAction import BseCorpActDBManager
from downloaders.namechange_model import NameChangeManager
from downloaders.bse_corpaction import BseCorpActDBManager


class DataManager:
    def __init__(self, session: db.orm.Session):
        self.session = session
        self.nameChange = NameChangeManager(session.get_bind())
        self.bseCorpAct = BseCorpActDBManager()

    def generate_symbol_id_filter(self, symbol_name: str, table):
        ids = self.nameChange.get_ids_of_symbol(symbol_name)
        return db.or_(table.symbol_id == i for i in ids)

    def adjust_equity_data(self, df: pd.DataFrame, ticker: str):
        acts = self.bseCorpAct.get_corp_actions(ticker)
        acts = acts[(acts["action"] == "bonus") | (acts["action"] == "split")]
        # filename = f"{str(datetime.now()).replace(':', '_')}  {ticker}"
        # df.to_csv(f"{filename}_before.csv")
        for index, row in acts.iterrows():
            if row.action == "split" or row.action == "bonus":
                df.loc[df.index < row.Ex_date] = df[
                    ["Open", "High", "Low", "Close"]
                ].div(row.C)
        # df.to_csv(f"{filename}_after.csv")
        return df

    def get_equity_data(
        self, symbol: str, fromDate: date = None, toDate: date = None, adjusted=True
    ):
        if not self.is_ticker_valid(symbol):
            return pd.DataFrame()
        series_query = db.select(Series.id).filter(Series.series_name.in_(("EQ", "BE")))
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
            .join(Symbol)
            .filter(Data.series_id.in_(series_query))
            .filter(self.generate_symbol_id_filter(symbol.upper(), Data))
            .order_by(
                # db.desc(Data.date1)
                Data.date1
            )
            .distinct(Data.date1)
        )

        filt = ()

        if fromDate:
            filt = filt + (
                Data.date1 >= datetime(fromDate.year, fromDate.month, fromDate.day),
            )

        if toDate:
            filt = filt + (
                Data.date1 <= datetime(toDate.year, toDate.month, toDate.day),
            )

        if filt != ():
            query = query.filter(*filt)
        # sql_stmt = str(query.statement.compile(compile_kwargs={"literal_binds": True}))
        df = pd.read_sql_query(query.statement, self.session.get_bind()).set_index(
            "Date"
        )
        if adjusted:
            self.adjust_equity_data(df, symbol)
            df["symbol"] = self.nameChange.get_latest_name(symbol)
        # print(df)
        return df

    def get_corpAction_data(self, symbol: str) -> pd.DataFrame:
        if self.is_ticker_valid(symbol):
            df = self.bseCorpAct.get_corp_actions(symbol)
            df.sort_values(by=["short_name", "Ex_date", "action"], inplace=True)
            return df
        else:
            return pd.DataFrame()

    def get_all_tickers(self) -> pd.DataFrame:
        query = (
            self.session.query(Symbol.symbol_name.label("symbol"))
            .order_by(Symbol.symbol_name)
            .distinct()
        )
        df = pd.read_sql_query(query.statement, self.session.get_bind())
        return df

    def plot_equity(
        self, symbol: str, fromDate: date = None, toDate: date = None, ax=None
    ) -> None:
        if self.is_ticker_valid(symbol):
            df = self.get_equity_data(symbol, fromDate=fromDate, toDate=toDate)
            # print(df.dtypes)
            if ax == None:
                mpf.plot(df, type="candle", mav=(50, 200))
            else:
                mpf.plot(df, type="candle", mav=(50, 200), ax=ax)

    def is_ticker_valid(self, ticker: str) -> bool:
        a = self.session.query(
            exists().where(Symbol.symbol_name == ticker.upper())
        ).scalar()
        return a


def test_get_corp_action():
    session = db.orm.Session(SQL_CON)
    dMgr = DataManager(session)
    df = dMgr.get_corpAction_data("TCS")
    print(df)
    df.to_csv("corp_data_test.csv")


def test_is_symbol_valid():
    session = db.orm.Session(SQL_CON)
    dMgr = DataManager(session)
    tickers = ["asdf", "Tcs", "TCS", "fffff", "HDFC"]
    for t in tickers:
        print(t, dMgr.is_ticker_valid(t))


def test_date_filter():
    session = db.orm.Session(SQL_CON)
    dMgr = DataManager(session)
    fromDate = date(2022, 1, 1)
    toDate = date(2023, 1, 10)
    df = dMgr.get_equity_data("TCS", fromDate=fromDate, toDate=toDate)
    print(df)


def main():
    session = db.orm.Session(SQL_CON)
    dMgr = DataManager(session)
    # dMgr.plot_equity("HINDUNILVR")
    # dMgr.plot_equity("YAARII")
    dMgr.plot_equity("INFY")
    print(dMgr.get_all_tickers())


def test_corp_action_parse():
    cMgr = BseCorpActDBManager()
    data = cMgr.get_corp_actions("INFY")
    print(data)


if __name__ == "__main__":
    main()
    # test_get_corp_action()
    # test_is_symbol_valid()
    # test_date_filter()
    test_corp_action_parse()
