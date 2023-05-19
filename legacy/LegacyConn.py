from google.cloud.sql.connector import Connector
from sqlalchemy import create_engine

db_user = "postgres"
db_pass = "mock_pwd"
db_name = "golddb"

# initialize Connector object
connector = Connector()

# function to return the database connection object
def getconn():
    conn = connector.connect(
        'gifted-loader-384715:asia-southeast1:ebd2023',
        "pg8000",
        user=db_user,
        password=db_pass,
        db=db_name
    )
    return conn

# create connection pool with 'creator' argument to our connection object function
alchemyEngine = create_engine(
    "postgresql+pg8000://",
    creator=getconn,
)

dbConnection = alchemyEngine.connect()