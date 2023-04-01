from google.cloud.sql.connector import Connector
from sqlalchemy import create_engine, text
import pandas as pd
import matplotlib.pyplot as plt

db_user = "postgres"
db_pass = "ebd2023"
db_name = "mockdb"
project_id = "ebd2023"
region = "asia-southeast1"
instance_name = "ebd2023"

#gcloud auth login

# initialize parameters
INSTANCE_CONNECTION_NAME = f"{project_id}:{region}:{instance_name}" # i.e demo-project:us-central1:demo-instance
print(f"Your instance connection name is: {INSTANCE_CONNECTION_NAME}")

# initialize Connector object
connector = Connector()

# function to return the database connection object
def getconn():
   conn = connector.connect(
       INSTANCE_CONNECTION_NAME,
       "pg8000",
       user=db_user,
       password=db_pass,
       db=db_name
   )
   return conn

# create connection pool with 'creator' argument to our connection object function
engine = create_engine(
   "postgresql+pg8000://",
   creator=getconn,
)

with engine.begin() as conn:
    #query = text("""SELECT module_presentation_length, code_presentation FROM courses""")

    query = text("""SELECT sa.date_submitted, sa.score
    FROM student_assessment sa join studentvle sv
    on sa.id_student = sv.id_student
    where (id_student, date_submitted) in
    (select id_student, max(date_submitted)
    from student_assessment group by id_student)""")

    df = pd.read_sql_query(query, conn)

    print(df)

    #df.plot.scatter(x='id_student', y='sum_click', s=100);

#plt.show()