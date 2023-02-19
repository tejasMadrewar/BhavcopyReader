import sqlalchemy as db
import pandas as pd

import config as cfg
import pd_model as model


class NameChangeManager():
    def __init__(self, engine):
        Session = db.orm.sessionmaker(bind=engine)
        self.session = Session()
        self.tbl = model.NameChange

    def download_data(self):
        url = "https://www1.nseindia.com/content/equities/symbolchange.csv"
        df = pd.read_csv(url, encoding="ISO-8859-1", header=None)
        df.columns = ["security", "old_symbol", "new_symbol", "date1"]
        df = df.astype(
            {"date1": "datetime64[ns]",
             "security": "string",
             "old_symbol": "string",
             "new_symbol": "string"})
        df = df.sort_values("date1")
        df = df[["date1", "security", "old_symbol", "new_symbol"]]
        df.to_csv("SymbolChanges.csv", index=False)
        return df

    def clean_data(self, df):
        sym = pd.read_sql_table(
            model.Symbol.__tablename__, cfg.SQL_CON)
        # left join 1
        df = df.merge(sym, how="left", left_on="old_symbol",
                      right_on="symbol_name").drop(columns=["symbol_name"])
        df = df.rename(columns={"id": "old_symbol_id"})
        # left join 2
        df = df.merge(sym, how="left", left_on="new_symbol",
                      right_on="symbol_name").drop(columns=["symbol_name"])
        df = df.rename(columns={"id": "new_symbol_id"})
        # remove symbols which are not present in symbol table
        df = df.dropna()
        return df

    def save_to_database(self, df, session):
        session.bulk_insert_mappings(
            model.NameChange, df.to_dict(orient="records"))
        session.commit()

    def update(self):
        df = self.download_data()
        df = self.clean_data(df)
        df.to_csv("symbolIdChanges.csv")
        self.save_to_database(df, self.session)

    def gen_query_next(self, symbol_id):
        query = self.session.query(
            self.tbl.old_symbol_id.label("symbols")).filter(
                (self.tbl.new_symbol_id == symbol_id)
        )
        return query

    def gen_query_prev(self, symbol_id):
        query = self.session.query(
            self.tbl.new_symbol_id.label("symbols")).filter(
                (self.tbl.old_symbol_id == symbol_id)
        )
        return query

    def get_all_symbol_ids(self, symbol_id):
        data = set()
        data.add(symbol_id)
        result = self.gen_query_next(symbol_id).all()
        while result != []:
            next = result[0][0]
            data.add(next)
            result = self.gen_query_next(next).all()
        result = self.gen_query_prev(symbol_id).all()
        while result != []:
            next = result[0][0]
            data.add(next)
            result = self.gen_query_prev(next).all()
        data = list(data)
        print(data)
        return data


def update():
    # nameChange.update()
    nameChange = NameChangeManager(cfg.SQL_CON)
    nameChange.get_all_symbol_ids(2175)
    nameChange.get_all_symbol_ids(2585)
    nameChange.get_all_symbol_ids(2685)  # YAARII
    nameChange.get_all_symbol_ids(2859)  # IBULISL


if __name__ == "__main__":
    update()
