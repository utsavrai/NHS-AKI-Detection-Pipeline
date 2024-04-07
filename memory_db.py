import sqlite3
from constants import ON_DISK_DB_PATH
import os
from utils import populate_test_results_table, populate_patients_table
import threading


class InMemoryDatabase:
    def __init__(self, history_load_path):
        self.on_disk_db_lock = threading.Lock()
        self.disk_db_being_accessed = False
        self.discharged_patient_mrns = {}
        self.connection = sqlite3.connect(":memory:")
        self.initialise_tables()
        self.load_db(history_load_path)
        # make sure we always have a db file
        if not os.path.exists(ON_DISK_DB_PATH):
            # create the directories if they don't already exist
            os.makedirs(
                "/".join(ON_DISK_DB_PATH.split("/")[:-1]), mode=0o700, exist_ok=True
            )
            # persist the database on-disk
            self.persist_db()

    def initialise_tables(self):
        """
        Initialise the database with the patient features table.
        """
        create_patients = """
            CREATE TABLE patients (
                mrn TEXT PRIMARY KEY,   
                age INTEGER,
                sex TEXT
            );
        """
        create_test_results = """
            CREATE TABLE test_results (
                mrn TEXT,   
                date DATETIME,
                result DECIMAL,
                PRIMARY KEY (mrn, date),
                FOREIGN KEY (mrn) REFERENCES patients (mrn)
            );
        """
        create_patient_features = """
            CREATE TABLE features (
                mrn TEXT PRIMARY KEY,   
                age INTEGER,
                sex TEXT,
                C1 DECIMAL,
                RV1 DECIMAL,
                RV1_ratio DECIMAL,
                RV2 DECIMAL,
                RV2_ratio DECIMAL,
                has_changed_48h INTEGER,
                D DECIMAL,
                aki TEXT
            );
        """
        # create the tables
        self.connection.execute(create_patients)
        self.connection.execute(create_test_results)
        self.connection.execute(create_patient_features)

    def insert_patient_features(
        self, mrn, age, sex, c1, rv1, rv1_r, rv2, rv2_r, change, D, aki=None
    ):
        """
        Insert the obtained features into the in-memory database.
        Args:
            - mrn {str}: Medical Record Number of the patient
            - age {int}: Age of the patient
            - sex {str}: Sex of the patient ('m'/'f')
            - c1 {float}: Most recent creatinine result value
            - rv1 {float}: Lowest creatinine result in last 7d
            - rv1_r {float}: C1 / RV1
            - rv2 {float}: Median creatinine result in within last 8-365d
            - rv2_r {float}: C1 / RV2
            - change {bool}: Whether there has been a change in last 48h
            - D {float}: Difference between current and lowest previous result (48h)
            - aki {str}: Whether the patient has been diagnosed with aki ('y'/'n')
        """
        query = """
            INSERT INTO features 
                (mrn, age, sex, C1, RV1, RV1_ratio, RV2, RV2_ratio, has_changed_48h, D, aki) 
            VALUES 
                (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        # execute the query
        try:
            self.connection.execute(
                query, (mrn, age, sex, c1, rv1, rv1_r, rv2, rv2_r, change, D, aki)
            )
            self.connection.commit()
        except sqlite3.IntegrityError:
            print(f"The features for patient {mrn} are already in the features table!")

    def insert_patient(self, mrn, age, sex, update_disk_db=True):
        """
        Insert the patient info from PAS into the in-memory database.
        Args:
            - mrn {str}: Medical Record Number of the patient
            - age {int}: Age of the patient
            - sex {str}: Sex of the patient ('m'/'f')
        """
        query = """
            INSERT INTO patients 
                (mrn, age, sex) 
            VALUES 
                (?, ?, ?)
        """
        # in case the patient was discharged before
        if mrn in self.discharged_patient_mrns:
            self.discharged_patient_mrns[mrn] = False
        # execute the query
        try:
            self.connection.execute(query, (mrn, age, sex))
            self.connection.commit()

        except sqlite3.IntegrityError:
            print(f"Patient {mrn} is already in the patients table!")

        # if update_disk_db:
        #     disk_conn = sqlite3.connect(ON_DISK_DB_PATH)
        #     disk_conn.execute('INSERT OR IGNORE INTO patients (mrn, age, sex) VALUES (?, ?, ?)', (mrn, age, sex))
        #     disk_conn.commit()
        #     disk_conn.close()

    def insert_test_result(self, mrn, date, result):
        """
        Insert the patient info from PAS into the in-memory database.
        Args:
            - mrn {str}: Medical Record Number of the patient
            - date {datetime}: creatinine result date
            - result {float}: creatinine result
        """
        query = """
            INSERT INTO test_results 
                (mrn, date, result) 
            VALUES 
                (?, ?, ?)
        """

        # execute the query
        try:
            self.connection.execute(query, (mrn, date, result))
            self.connection.commit()
        except sqlite3.IntegrityError:
            print(
                f"Test result on date-time: {date} for: {mrn} is already in the test_results table!"
            )

    def get_patient_features(self, mrn):
        """
        Query the features table for a given mrn.
        Args:
            - mrn {str}: Medical Record Number
        """
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM features WHERE mrn = ?", (mrn,))
        return cursor.fetchone()

    def database_loaded(self):
        """
        Query the patients table to check if it is currently loaded
        """
        cursor = self.connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM test_results")
        count = cursor.fetchone()[0]
        if count > 0:
            return True
        else:
            return False

    def get_patient(self, mrn):
        """
        Query the patients table for a given mrn.
        Args:
            - mrn {str}: Medical Record Number
        """
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM patients WHERE mrn = ?", (mrn,))
        return cursor.fetchone()

    def get_test_result(self, mrn, date):
        """
        Query the test result table for a given mrn and date.
        Args:
            - mrn {str}: Medical Record Number
            - date {str}: The date and time of the test
        """
        cursor = self.connection.cursor()
        cursor.execute(
            "SELECT * FROM test_results WHERE mrn = ? AND date = ?", (mrn, date)
        )
        return cursor.fetchone()

    def get_test_results(self, mrn):
        """
        Query the test results table for a given mrn.
        Args:
            - mrn {str}: Medical Record Number
        """
        cursor = self.connection.cursor()
        cursor.execute("SELECT * FROM test_results WHERE mrn = ?", (mrn,))
        return cursor.fetchall()

    def get_patient_history(self, mrn):
        """
        Get patient info along with all their test results and their dates.
        Args:
            - mrn {str}: Medical Record Number
        Returns:
            - _ {list}: List of records
        """
        query = """
            SELECT 
                patients.mrn,
                patients.age,
                patients.sex,
                test_results.date,
                test_results.result
            FROM
                patients
            JOIN
                test_results 
            ON
                patients.mrn = test_results.mrn
            WHERE patients.mrn = ?
        """
        cursor = self.connection.cursor()
        cursor.execute(query, (mrn,))
        return cursor.fetchall()

    def discharge_patient(self, mrn):
        """
        Remove the patient record from patients table in-memory and on-disk. Test
        results are kept in the test_results table for historic data.
        Args:
            - mrn {str}: Medical Record Number
        """
        # save to queue for on-disk sync
        self.discharged_patient_mrns[mrn] = True
        # delete from in-memory
        self.connection.execute("DELETE FROM patients WHERE mrn = ?", (mrn,))
        self.connection.commit()

    def execute_queued_operations(self, disk_connection):
        """
        Perform any queued operations on the on-disk database.
        """
        # delete the discharged patients
        print("Started executing queued operations.")
        for mrn in self.discharged_patient_mrns:
            if self.discharged_patient_mrns[mrn]:
                disk_connection.execute("DELETE FROM patients WHERE mrn = ?", (mrn,))
        disk_connection.commit()
        print("Finished commiting queued operations.")
        self.discharged_patient_mrns.clear()

    def update_patient_features(self, mrn, **kwargs):
        """
        Update patient information based on the provided keyword arguments.
        Args:
            - mrn {str}: Medical Record Number of the patient to update
            - **kwargs {dict}: Where key=column, value=new value
        """
        # construct the SET part of the SQL query based on the given args
        set_clause = ", ".join([f"{key} = ?" for key in kwargs])
        query = f"UPDATE features SET {set_clause} WHERE mrn = ?"
        # prepare the values for the placeholders in the SQL statement
        values = list(kwargs.values()) + [mrn]
        # execute the query
        self.connection.execute(query, values)
        self.connection.commit()

    def persist_db(self):
        """
        Persist the in-memory database to disk.
        Args:
            - disk_db_path {str}: the path to the database
        """
        # backs up and closes the connection
        self.connection.commit()
        with self.on_disk_db_lock:
            self.disk_db_being_accessed = True
            print("Lock acquired in persist_db.")
            with sqlite3.connect(ON_DISK_DB_PATH) as disk_connection:
                self.connection.backup(disk_connection)
                self.execute_queued_operations(disk_connection)
        self.disk_db_being_accessed = False
        print("Lock released in persist_db.")

    def load_db(self, history_load_path):
        """
        Load the on-disk database into the in-memory database.
        """
        # if on-disk db doesn't exist, use the csv file
        if not os.path.exists(ON_DISK_DB_PATH):
            print("Loading the history.csv file in memory.")
            populate_test_results_table(self, history_load_path)
            # populate_patients_table(self, 'processed_history.csv')
        else:
            # load the on-disk db into the in-memory one
            with self.on_disk_db_lock:
                self.disk_db_being_accessed = True
                print("Lock acquired in load_db.")
                with sqlite3.connect(ON_DISK_DB_PATH) as disk_connection:
                    print("Loading the on-disk database in memory.")
                    disk_connection.backup(self.connection)
            self.disk_db_being_accessed = False
            print("Lock released in load_db.")

    def close(self):
        """
        Close the database connection.
        """
        self.connection.close()
