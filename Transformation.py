import pandas as pd

def run_transformation():
    data = pd.read_csv('zipco_transaction.csv')

    # Remove duplicate entries
    data.drop_duplicates(inplace=True)

    # Replace missing values ( filling missing numeric values with mean or median)

    # Replace missing values ( filling missing numeric values with mean or median)
    numeric_columns = data.select_dtypes(include=['float64', 'int64']).columns
    for col in numeric_columns:
      data.fillna({col: data[col].mean()}, inplace=True)

    #Handle missing values  (fill missing string/objects values with "unknown")
    string_columns = data.select_dtypes(include=['object']).columns
    for col in string_columns:
        data.fillna({col: 'Unknown'}, inplace=True)

    # cleaning date column? assigning the corect data types
    data['Date'] = pd.to_datetime(data['Date'])

    # Create the product table 
    products = data[['ProductName']].drop_duplicates().reset_index(drop=True)
      # Add the table ID starting from 1
    products['Product_id'] = products.index + 1
    products = products[['Product_id', 'ProductName']]

    #Customer Table

    customers = data[['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber','CustomerEmail']].drop_duplicates().reset_index(drop=True)
    customers['Customer_id'] = customers.index + 1
    # Reorder the columns to make 'TableID' the first column
    customers = customers[['Customer_id', 'CustomerName', 'CustomerAddress', 'Customer_PhoneNumber','CustomerEmail']]

    # staff table
    staff = data[['Staff_Name', 'Staff_Email']].drop_duplicates().reset_index(drop=True)
    staff ['staffID'] = staff.index + 1
    staff = staff[['staffID','Staff_Name', 'Staff_Email']]

    # transaction table
    transaction = data.merge(products, on=["ProductName"], how='left') \
                      .merge(customers, on=['CustomerName', 'CustomerAddress', 'Customer_PhoneNumber','CustomerEmail'], how='left') \
                      .merge(staff, on=['Staff_Name', 'Staff_Email'], how='left')
    transaction['TransactionID'] = transaction.index + 1
    transaction = transaction.reset_index() \
                              [['Date','TransactionID', 'Quantity', 'UnitPrice', 'StoreLocation','PaymentType', 'PromotionApplied', 'Weather', 'Temperature', \
                                'StaffPerformanceRating', 'CustomerFeedback', 'DeliveryTime_min','OrderType','Customer_id','staffID','DayOfWeek','TotalSales'
                                ]]
    
    # saving data as csv file
    data.to_csv('clean_data.csv', index=False)
    customers.to_csv('customers.csv', index=False)
    staff.to_csv('staff.csv', index=False)
    products.to_csv('products.csv', index=False)
    transaction.to_csv('transaction.csv', index=False)

    print('Data Cleaning and Transformation completed succesfully')