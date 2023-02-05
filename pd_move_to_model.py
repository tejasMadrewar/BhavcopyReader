import pd_model as model
import sqlalchemy as db
import pandas as pd
import datetime


src_engine = db.create_engine(
    'postgresql+psycopg2://postgres:123456789@localhost:5432/NSE_DATA')
dst_engine = db.create_engine(
    'postgresql+psycopg2://postgres:123456789@localhost:5432/test')


def df_to_model(session, df):
    tables = [{"model": model.Symbol, "src_col": "symbol", "dst_col": "symbol_name"},
              {"model": model.Security1, "src_col": "security1",
                  "dst_col": "security_name"},
              {"model": model.Series, "src_col": "series", "dst_col": "series_name"},
              {"model": model.Mkt, "src_col": "mkt", "dst_col": "mkt_name"}]
    # replace NA values with "_"
    df["series"] = df["series"].fillna("_")
    df["security1"] = df["security1"].fillna("_")
    df["mkt"] = df["mkt"].fillna("_")
    # update series,mkt,symbol,security1 tables
    for t in tables:
        src_col = t["src_col"]
        dst_col = t["dst_col"]
        m = t["model"]
        updated_data = pd.DataFrame(
            df[src_col].unique(), columns=[dst_col])
        query = session.query(m).statement
        old_data = pd.read_sql_query(query, con=dst_engine)
        new_data = pd.merge(updated_data, old_data, on=dst_col,
                            how="left")
        # new rows
        new_data = new_data[new_data.isna().any(axis=1)]
        print(f"Found {len(new_data)} new value(s) in {src_col}.")
        new_data = new_data[[dst_col]]
        # if not new_data.empty:
        #    print(new_data)
        session.bulk_insert_mappings(m, new_data.to_dict(orient="records"))
        session.commit()
    # create new data with join
    for t in tables:
        src_col = t["src_col"]
        dst_col = t["dst_col"]
        m = t["model"]
        query = session.query(m).statement
        t1 = pd.read_sql_query(query, con=session.bind)
        t1 = t1.rename(columns={"id": f"{src_col.lower()}_id"})
        df = df.rename(columns={src_col: dst_col})
        df = pd.merge(df, t1, on=dst_col, how="left")

    print(
        f"New data {len(df.date1.unique())} days [{len(df)} rows]")
    old_dates = pd.read_sql_query(session.query(
        model.Data.date1).distinct().statement, con=dst_engine)
    if not old_dates.empty:
        # remove data with old dates and then insert
        old_dates["status"] = "old_date"
        df = pd.merge(df, old_dates, on="date1", how="left")
        df = df[df.status.isna()]
    df = df[["date1", "mkt_id", "series_id", "symbol_id",
             "security1_id", "open_price", "high_price",
             "low_price", "close_price"
             ]]
    # remove duplicate values
    df = df[~df.duplicated()]
    print(f"Found {len(df.date1.unique())} new days")
    session.bulk_insert_mappings(
        model.Data, df.to_dict(orient="records"))
    session.commit()


def get_raw_data(engine: db.engine, start_dt: datetime.datetime.date, end_dt: datetime.datetime.date):
    metadata = db.MetaData(engine)
    metadata.reflect()
    table = metadata.tables["raw_data"]
    end = max(start_dt, end_dt)
    start = min(start_dt, end_dt)
    query = db.select(
        table.c.date1.label("date1"),
        table.c.mkt.label("mkt"),
        table.c.series.label("series"),
        table.c.symbol.label("symbol"),
        table.c.security1.label("security1"),
        table.c.open_price.label("open_price"),
        table.c.high_price.label("high_price"),
        table.c.low_price.label("low_price"),
        table.c.close_price.label("close_price")).filter(
            table.c.date1 >= start, table.c.date1 <= end
    ).order_by(db.desc(table.c.date1))
    # .limit(100)
    # print(query)
    df = pd.read_sql_query(query, engine)
    return df


def main():
    Session = db.orm.sessionmaker(bind=dst_engine)
    session = Session()
    # model.Base.metadata.drop_all(dst_engine)
    model.Base.metadata.create_all(dst_engine)
    session.commit()
    first = datetime.datetime.now().date()
    # first = datetime.date(year=2012, month=1, day=1)
    last = datetime.date(year=2009, month=1, day=1)
    step = 366
    while last < first:
        first2 = first - datetime.timedelta(days=step)
        print("from:", first, "to:", first2)
        df_to_model(session, get_raw_data(src_engine, first, first2))
        first = first2


if __name__ == "__main__":
    main()
