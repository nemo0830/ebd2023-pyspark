# Define database connection properties
jdbcHost = "0.0.0.0"  # Cloud SQL Proxy listening address
jdbcPort = "1234"
jdbcDatabase = "golddb"
jdbcUsername = "postgres"
jdbcPassword = "mock_pwd"

db_pass = "ebd2023"

# Set JDBC URL for the PostgreSQL database using the Cloud SQL Proxy
jdbcUrl = f"jdbc:postgresql://{jdbcHost}:{jdbcPort}/{jdbcDatabase}"

# Define properties for the database connection
connectionProperties = {
    "user": jdbcUsername,
    "password": jdbcPassword,
    "driver": "org.postgresql.Driver"

}
project_id = "ebd2023"
region = "asia-southeast1"
instance_name = "ebd2023"

INSTANCE_CONNECTION_NAME = f"{project_id}:{region}:{instance_name}" # i.e demo-project:us-central1:demo-instance


