# Define database connection properties
jdbcHost = "0.0.0.0"  # Cloud SQL Proxy listening address
jdbcPort = "1234"
jdbcDatabase = "mockdb"
jdbcUsername = "postgres"
jdbcPassword = "mock_pwd"

# Set JDBC URL for the PostgreSQL database using the Cloud SQL Proxy
jdbcUrl = f"jdbc:postgresql://{jdbcHost}:{jdbcPort}/{jdbcDatabase}"

# Define properties for the database connection
connectionProperties = {
    "user": jdbcUsername,
    "password": jdbcPassword,
    "driver": "org.postgresql.Driver"
}