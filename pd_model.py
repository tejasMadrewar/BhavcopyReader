import datetime
import sqlalchemy as db
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class Symbol(Base):
    __tablename__ = "symbol"
    id = db.Column(db.SmallInteger, primary_key=True)
    symbol_name = db.Column(db.String(10), nullable=False)
    db.UniqueConstraint(symbol_name)

    @staticmethod
    def get_or_create(session, sym_name):
        try:  # check if obj present
            return session.query(Symbol).filter_by(
                symbol_name=sym_name).one()
        except:
            try:  # new Symbol
                a = Symbol(symbol_name=sym_name)
                session.add(a)
                session.commit()
                return a
            except Exception as e:
                session.rollback()
                print(e)

    def __repr__(self) -> str:
        return f"<Symbol({self.id}, {self.symbol_name})>"


class Security1(Base):
    __tablename__ = "security1"
    id = db.Column(db.SmallInteger, primary_key=True)
    security_name = db.Column(db.String(100))
    db.UniqueConstraint(security_name)

    @staticmethod
    def get_or_create(session, sec_name):
        try:  # check if obj present
            return session.query(Symbol).filter_by(
                security_name=sec_name).one()
        except:
            try:  # new Symbol
                a = Symbol(security_name=sec_name)
                session.add(a)
                session.commit()
                return a
            except Exception as e:
                session.rollback()
                print(e)

    def __repr__(self) -> str:
        return f"<Security1({self.id}, {self.security_name})>"


class Mkt(Base):
    __tablename__ = "mkt"
    id = db.Column(db.SmallInteger, primary_key=True)
    mkt_name = db.Column(db.String(4), nullable=False)
    db.UniqueConstraint(mkt_name)

    @staticmethod
    def get_or_create(session, mkt):
        try:  # check if obj present
            return session.query(Mkt).filter_by(
                mkt_name=mkt).one()
        except:
            try:  # new Symbol
                a = Mkt(mkt_name=mkt)
                session.add(a)
                session.commit()
                return a
            except Exception as e:
                session.rollback()
                print(e)

    def __repr__(self) -> str:
        return f"<Mkt({self.id}, {self.mkt_name})>"


class Series(Base):
    __tablename__ = "series"
    id = db.Column(db.SmallInteger, primary_key=True)
    series_name = db.Column(db.String(4), nullable=False)
    db.UniqueConstraint(series_name)

    @staticmethod
    def get_or_create(session, series):
        try:  # check if obj present
            return session.query(Series).filter_by(
                series_name=series).one()
        except:
            try:  # new Symbol
                a = Series(series_name=series)
                session.add(a)
                session.commit()
                return a
            except Exception as e:
                session.rollback()
                print(e)

    def __repr__(self) -> str:
        return f"<Series({self.id}, {self.series_name})>"


class Data(Base):
    __tablename__ = "data"
    id = db.Column(db.Integer, primary_key=True)
    date1 = db.Column(db.DateTime, nullable=False)
    mkt_id = db.Column(db.SmallInteger, db.ForeignKey(
        "mkt.id"), nullable=False)
    series_id = db.Column(db.SmallInteger, db.ForeignKey(
        "series.id"), nullable=False)
    symbol_id = db.Column(db.SmallInteger, db.ForeignKey(
        "symbol.id"), nullable=False)
    security1_id = db.Column(db.SmallInteger, db.ForeignKey(
        "security1.id"), nullable=False)
    open_price = db.Column(db.REAL)
    high_price = db.Column(db.REAL)
    low_price = db.Column(db.REAL)
    close_price = db.Column(db.REAL, nullable=False)
    db.UniqueConstraint(date1, mkt_id, series_id, symbol_id, security1_id)

    def get_or_create(session, dt1, mk_id: int, ser_id: int,
                      sym_id: int, sec_id: id, op: float, hi: float,
                      lo: float, cl: float):
        print(dt1, mk_id, ser_id, sym_id, op, hi, lo, cl)
        try:  # check if obj present
            # return session.query(Data).filter(
            #      Data.date1 == dt1 and Data.mkt_id == mk_id and
            #      Data.series_id == ser_id and Data.symbol_id == sym_id
            #     ).execute().one()
            return session.query(Data).filter_by(date1=dt1).filter_by(mkt_id=mk_id, series_id=ser_id, symbol_id=sym_id, security1_id=sec_id).one()
        except:
            try:  # new Data
                a = Data(date1=dt1, mkt_id=mk_id,
                         series_id=ser_id, symbol_id=sym_id,
                         open_price=op, high_price=hi,
                         low_price=lo, close_price=cl)
                session.add(a)
                session.commit()
                return a
            except Exception as e:
                session.rollback()
                print(e)

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
    old_symbol_id = db.Column(db.SmallInteger, db.ForeignKey(
        "symbol.id"), nullable=False)
    new_symbol_id = db.Column(db.SmallInteger, db.ForeignKey(
        "symbol.id"), nullable=False)
    db.UniqueConstraint(date1, old_symbol_id, new_symbol_id)


data_idx = db.Index("data_idx", Data.id, Data.date1,
                    Data.mkt_id, Data.series_id, Data.symbol_id)
series_idx = db.Index("series_idx", Series.id, Series.series_name)
security1_idx = db.Index("security1_idx", Security1.id,
                         Security1.security_name)
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


def main():
    engine = db.create_engine("sqlite:///data.db")
    # engine = db.create_engine(
    # 'postgresql+psycopg2://postgres:123456789@localhost:5432/temp')
    Session = sessionmaker(bind=engine)
    session = Session()
    Base.metadata.create_all(engine)

    # sym = Symbol.get_or_create(session, sym_name="TCS")
    # print(sym)
    # sym = Symbol.get_or_create(session, sym_name="INFY")
    # print(sym)

    # mkt = Mkt.get_or_create(session, mkt="Y")
    # print(mkt)
    # mkt = Mkt.get_or_create(session, mkt="N")
    # print(mkt)

    # s = Series.get_or_create(session, series="EQ")
    # print(s)
    # s = Series.get_or_create(session, series="BL")
    # print(s)

    # dt = datetime.datetime(year=2022, month=11, day=1)
    # print(dt)
    # d = Data.get_or_create(session, dt1=dt, mk_id=mkt.id, ser_id=s.id,
    #                       sym_id=sym.id, op=10, hi=20, lo=5, cl=15)

    # print(d)
    # d = Data.get_or_create(session, dt1=dt, mk_id=mkt.id, ser_id=s.id,
    #                       sym_id=sym.id, op=10, hi=20, lo=5, cl=15)
    # print(d)
    # dt = datetime.datetime(year=2022, month=11, day=2)

    # d = Data.get_or_create(session, dt1=dt, mk_id=mkt.id, ser_id=s.id,
    #                       sym_id=sym.id, op=10, hi=20, lo=5, cl=15)
    # print(d)

    # print(session.query(Data.id, Data.date1, Mkt.mkt_name, Series.series_name, Symbol.symbol_name, Data.open_price,
    #                    Data.high_price, Data.low_price, Data.close_price).join(Mkt).join(Symbol).join(Series).all())

    # print(session.query(Data.id, Data.date1, Mkt.mkt_name, Series.series_name, Symbol.symbol_name, Data.open_price,
    #                    Data.high_price, Data.low_price, Data.close_price).join(Mkt).join(Symbol).join(Series).statement)


if __name__ == "__main__":
    # main()
    pass
