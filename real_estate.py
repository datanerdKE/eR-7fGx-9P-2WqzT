# Import Packages
from bs4 import BeautifulSoup
from datetime import datetime 
import requests   
import pandas as pd 
import numpy as np
from tqdm import trange
pd.set_option('display.max_rows',1000)
import time
import random
from google.cloud import bigquery

# initialize client object
client = bigquery.Client()

sort_orders = [
    'propertytypes=houses,apartments-flats,townhouses',
    'propertytypes=houses,apartments-flats,townhouses&SortOrder=PriceDescending',
    'propertytypes=houses,apartments-flats,townhouses&SortOrder=PropertyType'
]

selected_order = random.choice(sort_orders)
print(selected_order)

# Initialize DataFrame outside the loop
bigdata = pd.DataFrame()

try:
    for page in trange(1,50):
        counties = ['nairobi']
        furnished = ['true','false']
        rental_rates = ['day','month','week','year']
            
        for county in counties:
            for furnish in furnished:
                for rate in rental_rates:
                        
                    # specify website url 
                    headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
                    url = 'https://www.property24.co.ke/property-to-rent-in-'+str(county)+'-p95?rentalterm='+str(rate)+'&isfurnished='+str(furnish)+'&' + str(selected_order) + '&Page=' + str(page)
                    req = requests.get(url, headers=headers, timeout=36).text 
                    soup = BeautifulSoup(req,'lxml')
                    listings = soup.find_all('span',class_='p24_content')

                    for listing in listings:
                        try:
                            property_title = listing.find('span',class_='p24_propertyTitle').text.strip()
                            property_description = listing.find('span', class_='p24_excerpt').text.strip()
                            feature_details = listing.find_all('span', class_='p24_featureDetails')
                            features = str({feature['title']: feature.text.strip() for feature in feature_details if 'title' in feature.attrs})
                            property_availability = 'For Rent'
                            property_location = listing.find('span',class_='p24_location').text.strip()
                            property_address = listing.find('span',class_='p24_address').text.strip()
                            try:
                                floor_size = listing.find('span', class_='p24_size', title='Floor Size').text.strip()
                            except Exception as e:
                                floor_size = np.NAN
                            furnished = str(furnish)
                            rental_rate = rate
                            property_price = listing.find('span',class_='p24_price').text.strip()
                            last_scraped = datetime.now()
                                
                            # Create DataFrame
                            data = pd.DataFrame({
                                    'county':[county],
                                    'property_title':[property_title],
                                    'property_description':[property_description],
                                    'features':[features],
                                    'property_availability':[property_availability],
                                    'property_location':[property_location],
                                    'property_address':[property_address],
                                    'floor_size':[floor_size],
                                    'furnished':[furnished],
                                    'rental_rate':[rental_rate],
                                    'property_price':[property_price],
                                    'last_scraped':[last_scraped]
                                })

                            # Append data to bigdata DataFrame
                            bigdata = pd.concat([bigdata,data],ignore_index=True)
                                
                        except Exception as e:
                            pass            

    # Handle Database Import Error
    table_id = 'project-adrian-julius-aluoch.real_estate_data.real_estate_data'
    job = client.load_table_from_dataframe(bigdata,table_id)
    max_retries = 10
    retries = 0
    while job.state != 'DONE' and retries < max_retries:
        time.sleep(4)
        job.reload()
        retries += 1
        print(f"Data Upload Status : {job.state}")

except Exception as e:
    pass

print(bigdata.shape)

# Initialize DataFrame outside the loop
bigdata = pd.DataFrame()

try:
    for page in trange(1,50):
        counties = ['nairobi']
        
        for county in counties:
            # specify website url 
            headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}
            url = 'https://www.property24.co.ke/property-for-sale-in-'+str(county)+'-p95?' + str(selected_order) + '&Page=' + str(page)
            req = requests.get(url, headers=headers, timeout=36).text 
            soup = BeautifulSoup(req,'lxml')
            listings = soup.find_all('span',class_='p24_content')

            for listing in listings:
                try:
                    property_title = listing.find('span',class_='p24_propertyTitle').text.strip()
                    property_description = listing.find('span', class_='p24_excerpt').text.strip()
                    feature_details = listing.find_all('span', class_='p24_featureDetails')
                    features = str({feature['title']: feature.text.strip() for feature in feature_details if 'title' in feature.attrs})
                    property_availability = 'For Sale'
                    property_location = listing.find('span',class_='p24_location').text.strip()
                    property_address = listing.find('span',class_='p24_address').text.strip()
                    try:
                        floor_size = listing.find('span',class_='p24_size').text.strip()
                    except Exception as e:
                        floor_size = np.NAN
                    property_price = listing.find('span',class_='p24_price').text.strip()
                    rental_rate = np.NAN
                    furnished = np.NAN
                    last_scraped = datetime.now()
                    
                    # Create DataFrame
                    data = pd.DataFrame({
                        'county':[county],
                        'property_title':[property_title],
                        'property_description':[property_description],
                        'features':[features],
                        'property_availability':[property_availability],
                        'property_location':[property_location],
                        'property_address':[property_address],
                        'floor_size':[floor_size],
                        'furnished':[furnished],
                        'rental_rate':[rental_rate],
                        'property_price':[property_price],
                        'last_scraped':[last_scraped]
                    })

                    # Append data to bigdata DataFrame
                    bigdata = pd.concat([bigdata,data],ignore_index=True)
                    
                except Exception as e:
                    pass   

    # Handle Database Import Error
    table_id = 'project-adrian-julius-aluoch.real_estate_data.real_estate_data'
    job = client.load_table_from_dataframe(bigdata,table_id)
    max_retries = 10
    retries = 0
    while job.state != 'DONE' and retries < max_retries:
        time.sleep(4)
        job.reload()
        retries += 1
        print(f"Data Upload Status : {job.state}")

except Exception as e:
    pass

print(bigdata.shape)

# Define SQL Query to Retrieve Real Estate Data from Google Cloud BigQuery
sql = (
       'SELECT *'
       'FROM `real_estate_data.real_estate_data`'
       )

# Run SQL Query
data = client.query(sql, timeout=36).to_dataframe()
print(f'Rows of Real Estate Data in Google BigQuery : {data.shape[0]:,.0f}\nCols of Real Estate Data in Google BigQuery : {data.shape[1]:,.0f}')

# Check Total Number of Duplicate Records
duplicated = data.duplicated(subset=[
       'county', 'property_title', 'property_description', 'features', 'property_availability',
       'property_location', 'property_address', 'floor_size', 
       'furnished', 'rental_rate', 'property_price'
                                    ]).sum()

# Remove Duplicate Records
data.drop_duplicates(subset=[
        'county', 'property_title', 'property_description', 'features', 'property_availability',
       'property_location', 'property_address', 'floor_size', 
       'furnished', 'rental_rate', 'property_price'
                            ],inplace=True)

# Display Initial & Final Number of Duplicate Records
print(f"Initial Shape of Dataset : {data.shape}\nTotal Duplicate Records : {duplicated:,.0f}\nFinal Shape of Dataset : {data.shape}")

# Drop Original Real Estate Table 
table_id = 'project-adrian-julius-aluoch.real_estate_data.real_estate_data'
client.delete_table(table_id)

# Upload Final Real Estate Table
job = client.load_table_from_dataframe(data,table_id)
max_retries = 10
retries = 0
while job.state != 'DONE' and retries < max_retries:
    time.sleep(4)
    job.reload()
    retries += 1
    print(f'Real Estate Data Update : {job.state}')
