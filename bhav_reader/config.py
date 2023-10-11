from sqlalchemy import create_engine

DOWNLOAD_FOLDER = "C:\\Users\\TEJAS\\Downloads\\PR00_bhavcopy_data_all\\"
# for sqlite db
# SQL_CON = create_engine("sqlite:///data.db")
SQL_CON = create_engine(
    "postgresql+psycopg2://postgres:123456789@localhost:5432/NSE_DATA"
)
