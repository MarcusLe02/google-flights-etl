import mysql.connector
import datetime
import csv

mydb = mysql.connector.connect(
        host='localhost',
        user='USERNAME',
        password='PASSWORD',
        database='google_flight'
)

mycursor = mydb.cursor()

# Uncomment these lines if this is the first time you upload the database
# companies = [("Vietnam Airlines", "VN"),
#              ("Bamboo Airways", "QH"),
#              ("Vietravel Airlines", "VU"),
#              ("VietJet Air", "VJ"),
#              ("Pacific Airlines", "BL")]
# for company in companies:
#         sql = "INSERT INTO companies (name, code_name) values (%s, %s)"
#         val = company     
#         mycursor.execute(sql, val)

# mydb.commit()

# Back-up add files from local CSV files
# current_date = datetime.datetime.now().strftime("%Y-%m-%d")
# with open('/Users/marcusle02/Documents/Learning/google_flight_etl/RT Flight Data/flight_data_2023-12-28.csv', 'r') as file:
#     csv_data = csv.reader(file)
#     next(csv_data)  # Skip the header row

#     for row in csv_data:
#         # SQL query to insert data
#         sql = """INSERT INTO flights (departure, arrival, date_leave, date_return, 
#                  depart_time, arrive_time, company, price, scraping_date) 
#                  VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
#         mycursor.execute(sql, row)

# # Commit the transaction
# mydb.commit()

# Close the cursor and connection
mycursor.close()
mydb.close()