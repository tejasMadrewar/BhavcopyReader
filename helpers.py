from pd_model import Base, Data, NameChange
import sqlalchemy as db
import datetime
import config as cfg


def get_last_updated_dates_of_model(session):
    query = session.query(db.func.max(Data.date1))
    dt = query.all()[0][0]
    if dt != None:
        print(f"Data: {dt.date()}")
    query = session.query(db.func.max(NameChange.date1))
    dt = query.all()[0][0]
    if dt != None:
        print(f"NameChange: {dt.date()}")


def main():
    Session = db.orm.sessionmaker(bind=cfg.SQL_CON)
    session = Session()
    Base.metadata.create_all(cfg.SQL_CON)
    get_last_updated_dates_of_model(session)


if __name__ == "__main__":
    main()
