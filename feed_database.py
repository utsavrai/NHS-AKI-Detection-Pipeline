import csv
import sqlite3

# Path to your CSV file
csv_file_path = './history.csv'

# SQLite database file
db_file_path = './database.db'

# Connect to the SQLite database (this will create the database if it doesn't exist)
conn = sqlite3.connect(db_file_path)
cursor = conn.cursor()

# Create a table in the database
# Adjust the column names and types according to your CSV file
#mrn,creatinine_date_0,creatinine_result_0,creatinine_date_1,creatinine_result_1,creatinine_date_2,creatinine_result_2,creatinine_date_3,creatinine_result_3,creatinine_date_4,creatinine_result_4,creatinine_date_5,creatinine_result_5,creatinine_date_6,creatinine_result_6,creatinine_date_7,creatinine_result_7,creatinine_date_8,creatinine_result_8,creatinine_date_9,creatinine_result_9,creatinine_date_10,creatinine_result_10,creatinine_date_11,creatinine_result_11,creatinine_date_12,creatinine_result_12,creatinine_date_13,creatinine_result_13,creatinine_date_14,creatinine_result_14,creatinine_date_15,creatinine_result_15,creatinine_date_16,creatinine_result_16,creatinine_date_17,creatinine_result_17,creatinine_date_18,creatinine_result_18,creatinine_date_19,creatinine_result_19,creatinine_date_20,creatinine_result_20,creatinine_date_21,creatinine_result_21,creatinine_date_22,creatinine_result_22,creatinine_date_23,creatinine_result_23,creatinine_date_24,creatinine_result_24,creatinine_date_25,creatinine_result_25,creatinine_date_26,creatinine_result_26
cursor.execute('''
    CREATE TABLE IF NOT EXISTS my_table (
        column1 mrn,
        column2 creatinine_date_0,
        column3 creatinine_result_0,
        column4 creatinine_date_1,
        column5 creatinine_result_1,
        column6 creatinine_date_2,
        column7 creatinine_result_2,
        column8 creatinine_date_3,
        column9 creatinine_result_3,
        column10 creatinine_date_4,
        column11 creatinine_result_4,
        column12 creatinine_date_5,
        column13 creatinine_result_5,
        column14 creatinine_date_6,
        column15 creatinine_result_6,
        column16 creatinine_date_7,
        column17 creatinine_result_7,
        column18 creatinine_date_8,
        column19 creatinine_result_8,
        column20 creatinine_date_9,
        column21 creatinine_result_9,
        column22 creatinine_date_10,
        column23 creatinine_result_10,
        column24 creatinine_date_11,
        column25 creatinine_result_11,
        column26 creatinine_date_12,
        column27 creatinine_result_12,
        column28 creatinine_date_13,
        column29 creatinine_result_13,
        column30 creatinine_date_14,
        column31 creatinine_result_14,
        column32 creatinine_date_15,
        column33 creatinine_result_15,
        column34 creatinine_date_16,
        column35 creatinine_result_16,
        column36 creatinine_date_17,
        column37 creatinine_result_17,
        column38 creatinine_date_18,
        column39 creatinine_result_18,
        column40 creatinine_date_19,
        column41 creatinine_result_19,
        column42 creatinine_date_20,
        column43 creatinine_result_20,
        column44 creatinine_date_21,
        column45 creatinine_result_21,
        column46 creatinine_date_22,
        column47 creatinine_result_22,
        column48 creatinine_date_23,
        column49 creatinine_result_23,
        column50 creatinine_date_24,
        column51 creatinine_result_24,
        column52 creatinine_date_25,
        column53 creatinine_result_25,
        column54 creatinine_date_26,
        column55 creatinine_result_26)
''')

# Open the CSV file
with open(csv_file_path, 'r') as csv_file:
    # Create a CSV reader object
    csv_reader = csv.reader(csv_file, delimiter=',')
    
    # Skip the header row if your CSV has one
    next(csv_reader)
    
    # Insert rows into the SQLite database
    for row in csv_reader:
        cursor.execute('''
            INSERT INTO my_table (column1, column2, column3, column4, column5, column6, column7, column8, column9, column10, 
                                  column11, column12, column13, column14, column15, column16, column17, column18, column19, column20,
                                  column21, column22, column23, column24, column25, column26, column27, column28, column29, column30, 
                                  column31, column32, column33, column34, column35, column36, column37, column38, column39, column40,
                                  column41, column42, column43, column44, column45, column46, column47, column48, column49, column40, 
                                  column51, column52, column53, column54, column55)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', row)

# Commit the transaction and close the connection
conn.commit()
conn.close()

print('CSV data successfully imported into SQLite database.')
