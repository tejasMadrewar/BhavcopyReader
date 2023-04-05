import pandas as pd
import sqlalchemy as db
import mplfinance as mpf

from pd_model import Symbol, Data, Mkt, Series, Security1
import config as cfg
import nameChangeModel as nameChange


class DataManager:
    def __init__(self, session: db.orm.Session):
        self.session = session
        self.nameChange = nameChange.NameChangeManager(session.get_bind())

    def setup_queries(self):
        query_get_symbol_info = ""

    def query_to_df(self, query):
        df = (pd.read_sql_query(query.statement, self.session.get_bind())
              .set_index("Date")
              )
        return df

    def generate_symbol_id_filter(self, symbol_name: str):
        ids = self.nameChange.get_ids_of_symbol(symbol_name)
        return db.or_(Data.symbol_id == i for i in ids)

    def get_equity_data(self, symbol: str, adjusted=True):
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
            self.generate_symbol_id_filter(symbol)
        ).order_by(
            # db.desc(Data.date1)
            Data.date1
        )
        # print(query)
        df = self.query_to_df(query)
        return df

    def plot_equity(self, symbol: str):
        df = self.get_equity_data(symbol)
        print(df)
        # print(df.dtypes)
        mpf.plot(df, type="candle", mav=(50, 200))


def main():
    session = db.orm.Session(cfg.SQL_CON)
    dMgr = DataManager(session)
    # dMgr.plot_equity("HINDUNILVR")
    # dMgr.plot_equity("YAARII")
    dMgr.plot_equity("INFY")


if __name__ == "__main__":
    main()
