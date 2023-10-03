import sqlalchemy as db
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Symbol(Base):
    __tablename__ = "symbol"
    # id = db.Column(db.SmallInteger, primary_key=True)
    id = db.Column(
        db.SmallInteger().with_variant(db.Integer, "sqlite"), primary_key=True
    )
    symbol_name = db.Column(db.String(10), nullable=False)
    db.UniqueConstraint(symbol_name)

    def __repr__(self) -> str:
        return f"<Symbol({self.id}, {self.symbol_name})>"


class Security1(Base):
    __tablename__ = "security1"
    # id = db.Column(db.SmallInteger, primary_key=True)
    id = db.Column(
        db.SmallInteger().with_variant(db.Integer, "sqlite"), primary_key=True
    )
    security_name = db.Column(db.String(100))
    db.UniqueConstraint(security_name)

    def __repr__(self) -> str:
        return f"<Security1({self.id}, {self.security_name})>"


class Mkt(Base):
    __tablename__ = "mkt"
    # id = db.Column(db.SmallInteger, primary_key=True)
    id = db.Column(
        db.SmallInteger().with_variant(db.Integer, "sqlite"), primary_key=True
    )
    mkt_name = db.Column(db.String(4), nullable=False)
    db.UniqueConstraint(mkt_name)

    def __repr__(self) -> str:
        return f"<Mkt({self.id}, {self.mkt_name})>"


class Series(Base):
    __tablename__ = "series"
    # id = db.Column(db.SmallInteger, primary_key=True)
    id = db.Column(
        db.SmallInteger().with_variant(db.Integer, "sqlite"), primary_key=True
    )
    series_name = db.Column(db.String(4), nullable=False)
    db.UniqueConstraint(series_name)

    def __repr__(self) -> str:
        return f"<Series({self.id}, {self.series_name})>"


class Data(Base):
    __tablename__ = "data"
    id = db.Column(db.Integer, primary_key=True)
    date1 = db.Column(db.DateTime, nullable=False)
    mkt_id = db.Column(db.SmallInteger, db.ForeignKey("mkt.id"), nullable=False)
    series_id = db.Column(db.SmallInteger, db.ForeignKey("series.id"), nullable=False)
    symbol_id = db.Column(db.SmallInteger, db.ForeignKey("symbol.id"), nullable=False)
    security1_id = db.Column(
        db.SmallInteger, db.ForeignKey("security1.id"), nullable=False
    )
    open_price = db.Column(db.REAL)
    high_price = db.Column(db.REAL)
    low_price = db.Column(db.REAL)
    close_price = db.Column(db.REAL, nullable=False)
    db.UniqueConstraint(date1, mkt_id, series_id, symbol_id, security1_id)

    def __repr__(self) -> str:
        return f"<data({self.id} {self.date1}, {self.mkt_id}, \
            {self.series_id}, {self.symbol_id}, \
            {self.open_price}, {self.high_price}, \
            {self.low_price}, {self.close_price} \
        )>"


class NameChange(Base):
    __tablename__ = "name_change"
    id = db.Column(db.Integer, primary_key=True)
    date1 = db.Column(db.DateTime, nullable=False)
    old_symbol_id = db.Column(
        db.SmallInteger, db.ForeignKey("symbol.id"), nullable=False
    )
    new_symbol_id = db.Column(
        db.SmallInteger, db.ForeignKey("symbol.id"), nullable=False
    )
    db.UniqueConstraint(date1, old_symbol_id, new_symbol_id)


class CorpAction(Base):
    __tablename__ = "corp_actions"
    id = db.Column(db.Integer, primary_key=True)
    date1 = db.Column(db.DateTime, nullable=False)
    series_id = db.Column(db.SmallInteger, db.ForeignKey("series.id"), nullable=False)
    symbol_id = db.Column(db.SmallInteger, db.ForeignKey("symbol.id"), nullable=False)
    record_dt = db.Column(db.DateTime)
    bc_strt_dt = db.Column(db.DateTime)
    bc_end_dt = db.Column(db.DateTime)
    ex_dt = db.Column(db.DateTime)
    nd_strt_dt = db.Column(db.DateTime)
    nd_end_dt = db.Column(db.DateTime)
    purpose = db.Column(db.Text, nullable=False)
    # db.UniqueConstraint(date1, series_id, symbol_id, record_dt,
    # bc_strt_dt, bc_end_dt, ex_dt, nd_strt_dt, nd_end_dt, purpose)


data_idx = db.Index(
    "data_idx", Data.id, Data.date1, Data.mkt_id, Data.series_id, Data.symbol_id
)
series_idx = db.Index("series_idx", Series.id, Series.series_name)
security1_idx = db.Index("security1_idx", Security1.id, Security1.security_name)
mkt_idx = db.Index("mkt_idx", Mkt.id, Mkt.mkt_name)
symbol_idx = db.Index("sym_idx", Symbol.id, Symbol.symbol_name)


def create_indexes(engine):
    print("Creating Indexes..")
    data_idx.create(engine)
    series_idx.create(engine)
    mkt_idx.create(engine)
    symbol_idx.create(engine)


def drop_indexes(engine):
    print("Creating Indexes..")
    data_idx.drop(engine)
    series_idx.drop(engine)
    mkt_idx.drop(engine)
    symbol_idx.drop(engine)


def get_last_updated_dates(session):
    query = session.query(db.func.max(Data.date1))
    d = query.all()[0][0]
    if d != None:
        print("Data last updated on : {d}")


def main():
    pass


if __name__ == "__main__":
    main()
