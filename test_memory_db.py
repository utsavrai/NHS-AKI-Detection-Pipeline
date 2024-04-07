import unittest
from memory_db import InMemoryDatabase
from datetime import datetime

class TestInMemoryDatabase(unittest.TestCase):
    def setUp(self):
        """
        Initialises the database before each test.
        """
        self.db = InMemoryDatabase('data/history.csv')


    def tearDown(self):
        """
        Closes the database after each test.
        """
        self.db.close()


    def test_insert_and_get_for_patient_features(self):
        actual_record = ('31251122', 42, 'm', 142.22, 127.45, 1.12, 156.89, 0.91, False, 0, None)
        # insert
        self.db.insert_patient_features(*actual_record)
        # get
        queried_record = self.db.get_patient_features('31251122')
        self.assertEqual(actual_record, queried_record)


    def test_insert_and_update_for_patient_features(self):
        actual_record = ['31251122', 42, 'm', 142.22, 127.45, 1.12, 156.89, 0.91, False, 0, None]
        # insert
        self.db.insert_patient_features(*actual_record)
        # update
        self.db.update_patient_features('31251122', RV1=114.98, RV1_ratio=1.24)
        actual_record[4] = 114.98
        actual_record[5] = 1.24
        # get patient after update
        queried_record = self.db.get_patient_features('31251122')
        self.assertEqual(tuple(actual_record), queried_record)


    def test_insert_and_get_for_patients(self):
        actual_record = ('0012352', 29, 'f')
        # insert
        self.db.insert_patient(*actual_record)
        # get
        queried_record = self.db.get_patient('0012352')
        self.assertEqual(actual_record, queried_record)


    def test_insert_and_get_for_test_result(self):
        date = str(datetime.today())
        actual_record = ('0012352', date, 109.43)
        # insert
        self.db.insert_test_result(*actual_record)
        # get
        queried_record = self.db.get_test_result('0012352', date)
        self.assertEqual(actual_record, queried_record)

    
    def test_insert_and_get_for_test_results(self):
        actual_record = ('0012352', str(datetime.today()), 109.43)
        # insert
        self.db.insert_test_result(*actual_record)
        # get
        queried_record = self.db.get_test_results('0012352')[0]
        self.assertEqual(actual_record, queried_record)

    
    def test_get_patient_history(self):
        date = str(datetime.today())
        patient = ['0012352', 29, 'f']
        test_result = ['0012352', date, 109.43]
        # insert
        self.db.insert_patient(*patient)
        self.db.insert_test_result(*test_result)
        # get
        queried_record = self.db.get_patient_history(patient[0])[0]
        self.assertEqual(tuple(patient + test_result[1:]), queried_record)


    def test_discharge_patient(self):
        patient = ['0012352', 29, 'f']
        # insert
        self.db.insert_patient(*patient)
        # discharge
        self.db.discharge_patient(patient[0])
        # get patient
        queried_record = self.db.get_patient(patient[0])
        self.assertIsNone(queried_record)


if __name__ == '__main__':
    unittest.main()