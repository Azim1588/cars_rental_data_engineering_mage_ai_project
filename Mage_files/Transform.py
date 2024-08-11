import pandas as pd
from faker import Faker

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

fake = Faker()

@transformer
def transform(df, *args, **kwargs):
    # Ensure that required columns are present
    required_columns = ['Rental_Start_Date', 'Rental_End_Date', 'Rental_ID', 
                        'Rental_Duration', 'Rental_Fee', 'Pickup_Location', 
                        'Dropoff_Location', 'Agent_ID', 'Agent_Name', 
                        'Customer_ID', 'Car_ID', 'Location_ID', 
                        'Payment_ID', 'Branch_Name', 'City', 
                        'State', 'Country', 'First_Name', 
                        'Last_Name', 'Email', 'Phone_Number', 
                        'Address', 'License_Number', 'Membership_Status', 
                        'Preferred_Vehicle_Type', 'VIN', 'Make', 'Model', 'Year', 
                        'License_Plate']
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise KeyError(f"Missing columns in input DataFrame: {', '.join(missing_columns)}")

    # Convert date columns to datetime
    df['Rental_Start_Date'] = pd.to_datetime(df['Rental_Start_Date'], errors='coerce')
    df['Rental_End_Date'] = pd.to_datetime(df['Rental_End_Date'], errors='coerce')

    # Check if conversion was successful
    if df['Rental_Start_Date'].isnull().any() or df['Rental_End_Date'].isnull().any():
        raise ValueError("Date conversion failed for some rows.")

    datetime_dim = df[['Rental_Start_Date', 'Rental_End_Date']].reset_index(drop=True)
    datetime_dim['Rental_Start_day'] = datetime_dim['Rental_Start_Date'].dt.day
    datetime_dim['Rental_Start_month'] = datetime_dim['Rental_Start_Date'].dt.month
    datetime_dim['Rental_Start_year'] = datetime_dim['Rental_Start_Date'].dt.year
    datetime_dim['Rental_End_day'] = datetime_dim['Rental_End_Date'].dt.day
    datetime_dim['Rental_End_month'] = datetime_dim['Rental_End_Date'].dt.month
    datetime_dim['Rental_End_year'] = datetime_dim['Rental_End_Date'].dt.year
    datetime_dim['datetime_id'] = datetime_dim.index
    datetime_dim.columns = [
        'Rental_Start_Date', 'Rental_End_Date', 'Rental_Start_day',
        'Rental_Start_month', 'Rental_Start_year', 'Rental_End_day',
        'Rental_End_month', 'Rental_End_year', 'datetime_id'
    ]
    datetime_dim = datetime_dim[[
        'datetime_id', 'Rental_Start_Date', 'Rental_Start_day',
        'Rental_Start_month', 'Rental_Start_year', 'Rental_End_Date', 
        'Rental_End_day', 'Rental_End_month', 'Rental_End_year'
    ]]

    rental_info_dim = df[['Rental_ID', 'Rental_Start_Date', 'Rental_End_Date', 
                          'Rental_Duration', 'Rental_Fee', 
                          'Pickup_Location', 'Dropoff_Location', 
                          'Agent_ID', 'Agent_Name']].copy()
    rental_info_dim.columns = ['Rental_ID', 'Rental_Start_Date', 'Rental_End_Date', 
                               'Rental_Duration (days)', 'Rental_Fee', 
                               'Pickup_Location', 'Dropoff_Location', 
                               'Agent_ID', 'Agent_Name']

    customer_dim = df[['Customer_ID', 'First_Name', 'Last_Name', 
                       'Email', 'Phone_Number', 'Address', 
                       'License_Number', 'Membership_Status', 
                       'Preferred_Vehicle_Type']].copy()
    customer_dim.columns = ['Customer_ID', 'First_Name', 'Last_Name', 
                            'Email', 'Phone_Number', 'Address', 
                            'License_Number', 'Membership_Status', 
                            'Preferred_Vehicle_Type']

    car_dim = df[['Car_ID', 'Make', 'Model', 'Year', 'License_Plate', 'VIN']]
    car_dim.columns = ['Car_ID', 'Make', 'Model', 'Year', 'License_Plate', 'VIN']

    location_dim = df[['Location_ID', 'Branch_Name', 'City', 'State', 'Country']].copy()
    location_dim.columns = ['Location_ID', 'Branch_Name', 'City', 'State', 'Country']

    payment_dim = df[['Payment_ID', 'Payment_Type', 'Amount', 'Payment_Date']]
    payment_dim.columns = ['Payment_ID', 'Payment_Type', 'Amount', 'Payment_Date']

    Agent_dim = df[['Agent_ID', 'Agent_Name']]

# Rename the columns to match the desired schema
    Agent_dim.columns = ['Agent_ID', 'Agent_Name']

    # Perform merges with dimension tables
    fact_table = df.merge(customer_dim, on='Customer_ID', how='left') \
              .merge(car_dim, on='Car_ID', how='left') \
              .merge(rental_info_dim, on='Rental_ID', how='left') \
              

# Select only the required columns for the final fact_table
    fact_table = fact_table[['Rental_ID', 'Customer_ID']]





    return "sucess"
