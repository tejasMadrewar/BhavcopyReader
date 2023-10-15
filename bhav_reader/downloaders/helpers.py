from Model import Base, Data, NameChange
import sqlalchemy as db
from config import DEFAULT_ENGINE


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
    Session = db.orm.sessionmaker(bind=DEFAULT_ENGINE)
    session = Session()
    Base.metadata.create_all(DEFAULT_ENGINE)
    get_last_updated_dates_of_model(session)


if __name__ == "__main__":
    main()
