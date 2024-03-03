import mysql.connector
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pandas as pd
import datetime
import time

CURRENT_DATE = datetime.datetime.now().strftime("%Y-%m-%d")

def connect_mysql(host, user, pw, db):

    mydb = mysql.connector.connect(
            host=host,
            user=user,
            password=pw,
            database=db
    )
    print("Connected to MySQL")
    return mydb

def scrape(mydb, departure_city, arrival_city, date_leave, date_return, max_retries=5):
    retries = 0

    while retries < max_retries:
        web = f"https://www.google.com/travel/flights?q=Flights%20to%20{arrival_city}%20from%20{departure_city}%20on%20{date_leave}%20through%20{date_return}"
        driver = webdriver.Chrome()
        driver.get(web)
        driver.maximize_window()

        # Lists to hold all flight data
        all_departure_times = []
        all_arrival_times = []
        all_companies = []
        all_prices = []

        flight_entries = driver.find_elements(By.CSS_SELECTOR, 'div.yR1fYc')

        for entry in flight_entries:
            try:
                departure_time = entry.find_element(By.CSS_SELECTOR, "span[aria-label^='Departure time:']").text
                departure_time = convert_time(departure_time)
                all_departure_times.append(departure_time)

                arrival_time = entry.find_element(By.CSS_SELECTOR, "span[aria-label^='Arrival time:']").text
                arrival_time = convert_time(arrival_time)
                all_arrival_times.append(arrival_time)

                company = entry.find_element(By.CSS_SELECTOR, '.sSHqwe.tPgKwe.ogfYpf > span').text
                all_companies.append(company)

                price = entry.find_element(By.CSS_SELECTOR, '.YMlIz.FpEdX > span').text
                if price == 'Price unavailable':
                    price = None
                else:
                    price = price[1:] # Remove currency symbol
                all_prices.append(price)

                # Insert data into MySQL:
                mycursor = mydb.cursor()
                sql = """INSERT INTO flights (departure, arrival, date_leave, date_return, 
                depart_time, arrive_time, company, price, scraping_date) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                val = (departure_city, arrival_city, date_leave, date_return, departure_time,
                       arrival_time, company, price, CURRENT_DATE)
                mycursor.execute(sql, val)

            except Exception as e:
                print(f"Exception: {e}")
                continue

        driver.quit()

        print(len(all_departure_times))
        print(len(all_arrival_times))
        print(len(all_companies))
        print(len(all_prices))

        if all_departure_times and all_arrival_times and all_companies and all_prices:
            # Create a DataFrame from the collected flight data
            flights_df = pd.DataFrame({
                'Departure City': [departure_city] * len(all_departure_times),
                'Arrival City': [arrival_city] * len(all_departure_times),
                'Date Leave': [date_leave] * len(all_departure_times),
                'Date Return': [date_return] * len(all_departure_times),
                'Departure Time': all_departure_times,
                'Arrival Time': all_arrival_times,
                'Company': all_companies,
                'Price': all_prices
            })

            mydb.commit()
            print(f'Finish scraping for {departure_city},{arrival_city},{date_leave},{date_return}')
            return flights_df
        else:
            print(f'Retrying scraping for {departure_city},{arrival_city},{date_leave},{date_return}')
            retries += 1

            time.sleep(2)

    print(f'Max retries reached for {departure_city},{arrival_city},{date_leave},{date_return}')
    return None  

def convert_time(time_str):
    if "+1" in time_str:
        time_str = time_str[:-2]
    # Check if the time_str contains "AM"
    if "AM" in time_str:
        return time_str.replace("AM", "").strip()
    elif "PM" in time_str:
        # Remove "PM" and split the time into hours and minutes
        time_parts = time_str.replace("PM", "").strip().split(":")
        if len(time_parts) == 2:
            # Convert the hour part to an integer and add 12 to it
            hour = int(time_parts[0]) + 12
            # Ensure the hour is in 24-hour format (0-23)
            hour %= 24
            # Construct and return the modified time string
            return f"{hour:02d}:{time_parts[1]}"
    
    return time_str

def multi_scrape(mydb, departure_city, arrival_city, dates_leave, dates_return):
    results = []
    for dl in dates_leave:
        for dr in dates_return:
            result = scrape(mydb, departure_city, arrival_city, dl, dr)
            results.append(result)
    return results


if __name__ == "__main__":

    mydb = connect_mysql(host='localhost', user='root', pw='5nam', db='google_flight')

    # Define the date combinations amd cities you want to scrape
    departure_city = 'HCM'
    arrival_city = 'HAN'
    dates_leave = ['2024-02-05', '2024-02-06', '2024-02-07']
    dates_return = ['2024-02-16', '2024-02-17', '2024-02-18']

    results = multi_scrape(mydb, departure_city, arrival_city, dates_leave, dates_return)

    # Concatenate all the results into a single DataFrame
    final_df = pd.concat(results, ignore_index=True)
    final_df['Scraping Date'] = CURRENT_DATE
    file_name = f"//Users/marcusle02/Documents/Learning/hadoop_big_data/google_flight_etl/daily_flight_data/flight_data_v2_{CURRENT_DATE}.csv"

    # Save a copy of scraping data
    final_df.to_csv(file_name, index=False, encoding='utf-8')

    print(f"Data saved to {file_name}")