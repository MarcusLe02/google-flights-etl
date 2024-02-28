import mysql.connector
import pandas as pd

connection = mysql.connector.connect(
        host='localhost',
        user='marcus',
        password='5nam',
        database='google_flight'
)

query = "SELECT * FROM flights"

df = pd.read_sql(query, connection)

connection.close()

# Delete first char if it displays currency
df['price'] = df['price'].apply(lambda x: x[1:] if x != "N/A" and not x[0].isdigit() else x)

df.to_csv("all_flights.csv", index=False)