from memory_db import InMemoryDatabase
import sqlite3
memory_db = InMemoryDatabase('data/history.csv')

disk_conn = sqlite3.connect('/state/database.db')
cursor = disk_conn.cursor()

patients = [['822825', 20, 'f'], ['522854', 42, 'm'], ['65289', 56, 'f']]

for patient in patients:
    memory_db.insert_patient(*patient)

memory_db.persist_db()

connection = sqlite3.connect('/state/database.db')
disk_cursor = disk_conn.cursor()
memory_cursor = memory_db.connection.cursor()

# test present tables
get_tables_query = "SELECT name FROM sqlite_master WHERE type='table'"
expected_tables = memory_cursor.execute(get_tables_query).fetchall()
actual_tables = disk_cursor.execute(get_tables_query).fetchall()

# test present patients
count_all_patients_query = "SELECT Count(mrn) FROM patients"
expected_num_patients = memory_cursor.execute(count_all_patients_query).fetchone()
actual_num_patients = disk_cursor.execute(count_all_patients_query).fetchone()

# test present test results
count_all_test_results_query = "SELECT Count(mrn) FROM test_results"
expected_num_results = memory_cursor.execute(count_all_test_results_query).fetchone()
actual_num_results = disk_cursor.execute(count_all_test_results_query).fetchone()

# double check the patients are the same
get_all_patients_query = "SELECT * FROM patients LIMIT 10"
expected_patients = memory_cursor.execute(get_all_patients_query).fetchall()
actual_patients = disk_cursor.execute(get_all_patients_query).fetchall()

# double check the test results are the same
get_all_results_query = "SELECT * FROM test_results LIMIT 10"
expected_results = memory_cursor.execute(get_all_results_query).fetchall()
actual_results = disk_cursor.execute(get_all_results_query).fetchall()

print(f"Expected tables in the schema: {expected_tables}")
print(f"Actual tables in the schema: {actual_tables}")
print(f"Number of expected patients: {expected_num_patients}")
print(f"Number of actual patients: {actual_num_patients}")
print(f"Number expected of test_results: {expected_num_results}")
print(f"Number actual of test_results: {actual_num_results}")
print(f"Expected patients: {expected_patients}")
print(f"Actual patients: {actual_patients}")
print(f"Expected test results: {expected_results}")
print(f"Actual test results: {actual_results}")

# cleanup
for patient in patients:
    mrn = patient[0]
    memory_db.discharge_patient(mrn)

memory_db.persist_db()
disk_cursor.close()
connection.close()
memory_cursor.close()