import sqlalchemy as db
import pandas as pd

import config as cfg
from pd_model import Base, Symbol, NameChange


class NameChangeManager:
    def __init__(self, engine):
        Session = db.orm.sessionmaker(bind=engine)
        self.session = Session()

    def download_data(self):
        url = "https://www1.nseindia.com/content/equities/symbolchange.csv"
        url = "https://archives.nseindia.com/content/equities/symbolchange.csv"
        df = pd.read_csv(url, encoding="ISO-8859-1", header=None)
        df.columns = ["security", "old_symbol", "new_symbol", "date1"]
        df = df.astype(
            {
                "date1": "datetime64[ns]",
                "security": "string",
                "old_symbol": "string",
                "new_symbol": "string",
            }
        )
        df = df.sort_values("date1")
        df = df[["date1", "security", "old_symbol", "new_symbol"]]
        df.to_csv("symbolchange.csv", index=False)
        return df

    def clean_data(self, df: pd.DataFrame):
        sym = pd.read_sql_table(Symbol.__tablename__, cfg.SQL_CON)
        # left join 1
        df = df.merge(
            sym, how="left", left_on="old_symbol", right_on="symbol_name"
        ).drop(columns=["symbol_name"])
        df = df.rename(columns={"id": "old_symbol_id"})
        # left join 2
        df = df.merge(
            sym, how="left", left_on="new_symbol", right_on="symbol_name"
        ).drop(columns=["symbol_name"])
        df = df.rename(columns={"id": "new_symbol_id"})
        # remove symbols which are not present in symbol table
        df = df.dropna()
        return df

    def save_to_database(self, df: pd.DataFrame, session):
        # remove old dates
        Base.metadata.create_all(session.bind)
        # delete all previous data
        session.query(NameChange).delete()
        session.commit()
        session.bulk_insert_mappings(NameChange, df.to_dict(orient="records"))
        session.commit()
        print("Updated name_change table")

    def update(self):
        df = self.download_data()
        df = self.clean_data(df)
        self.save_to_database(df, self.session)

    def gen_query_next(self, symbol_id: str):
        query = self.session.query(NameChange.old_symbol_id.label("symbols")).filter(
            (NameChange.new_symbol_id == symbol_id)
        )
        return query

    def gen_query_prev(self, symbol_id: str):
        query = self.session.query(NameChange.new_symbol_id.label("symbols")).filter(
            (NameChange.old_symbol_id == symbol_id)
        )
        return query

    def get_all_symbol_ids(self, symbol_id: str):
        if symbol_id == None:
            return []
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
        return data

    def get_latest_name(self, symbol: str):
        ids = self.get_ids_of_symbol(symbol)
        query = (
            self.session.query(NameChange)
            .filter(NameChange.new_symbol_id.in_(ids))
            .order_by(db.desc(NameChange.date1))
            .limit(1)
        )
        df = pd.read_sql_query(query.statement, self.session.get_bind())
        if len(df) != 0:
            id = [int(df.iloc[0, 3])]
            return self.sym_ids_to_sym_names(id)[0]
        else:
            return symbol

    def sym_ids_to_sym_names(self, symbol_ids: list) -> list:
        query = self.session.query(Symbol).filter(Symbol.id.in_(symbol_ids))
        df = pd.read_sql_query(query.statement, self.session.get_bind())
        return df["symbol_name"].to_list()

    def get_id_of_symbol(self, symbol_name: str):
        query = self.session.query(Symbol.id.label("symbols")).filter(
            (Symbol.symbol_name == symbol_name)
        )
        result = query.all()
        if result == []:
            return None
        else:
            return result[0][0]

    def get_ids_of_symbol(self, symbol_name: str):
        return self.get_all_symbol_ids(self.get_id_of_symbol(symbol_name))


def test():
    symMgr = NameChangeManager(cfg.SQL_CON)
    l = symMgr.get_ids_of_symbol("INFY")
    print(symMgr.sym_ids_to_sym_names(l))
    print(symMgr.get_latest_name("INFY"))

    l = symMgr.get_ids_of_symbol("YAARII")
    print(symMgr.sym_ids_to_sym_names(l))
    print(symMgr.get_latest_name("YAARII"))

    l = symMgr.get_ids_of_symbol("IBULISL")
    print(symMgr.sym_ids_to_sym_names(l))
    print(symMgr.get_latest_name("IBULISL"))

    l = symMgr.get_ids_of_symbol("AGCNET")
    print(symMgr.sym_ids_to_sym_names(l))
    print(symMgr.get_latest_name("AGCNET"))

    l = symMgr.get_ids_of_symbol("TCS")
    print(symMgr.sym_ids_to_sym_names(l))
    print(symMgr.get_latest_name("TCS"))


def update():
    print("Updating nameChangeModel")
    nameChange = NameChangeManager(cfg.SQL_CON)
    nameChange.update()


if __name__ == "__main__":
    update()
