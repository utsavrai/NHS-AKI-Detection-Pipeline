import unittest
from utils import (
    process_mllp_message,
    parse_hl7_message,
    create_acknowledgement,
    populate_test_results_table,
    calculate_age,
    parse_system_message,
    strip_url,
    read_from_mllp,
)
from memory_db import InMemoryDatabase
import hl7
from constants import MLLP_END_OF_BLOCK
from unittest.mock import patch
from datetime import datetime


class TestUtilsClient(unittest.TestCase):
    def test_process_mllp_message(self):
        """
        Test processing of MLLP messages.
        """
        mllp_message = b"\x0bMSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240212131600||ADT^A01|||2.5\x1c\x0d"
        expected_result = (
            b"MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240212131600||ADT^A01|||2.5"
        )
        self.assertEqual(process_mllp_message(mllp_message), expected_result)

    def test_parse_hl7_message(self):
        """
        Test parsing of HL7 messages.
        """
        hl7_message = (
            "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240212131600||ADT^A01|||2.5"
        ).encode()
        parsed_message = parse_hl7_message(hl7_message)
        self.assertIsInstance(parsed_message, hl7.Message)
        self.assertTrue("MSH" in str(parsed_message))

    def test_create_acknowledgement(self):
        """
        Test creation of HL7 ACK messages.
        """
        hl7_msg = hl7.parse(
            "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240212131600||ADT^A01|||2.5"
        )
        ack_message = create_acknowledgement()
        self.assertIn(b"MSH", ack_message)
        self.assertIn(b"ACK", ack_message)
        self.assertIn(b"MSA|AA|", ack_message)

    def test_populate_test_results_table(self):
        db = InMemoryDatabase("70102-peace/data/history.csv")
        populate_test_results_table(db, "history.csv")
        # expected result
        expected_result = ("822825", "2024-01-01 06:12:00", 68.58)
        result = db.get_test_results(expected_result[0])[0]
        # close the db
        db.close()
        self.assertEqual(result, expected_result)

    def test_pas_admit_message(self):
        # Mock a PAS admit HL7 message
        message = "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240924102800||ADT^A01|||2.5\nPID|1||722269||SAFFRON CURTIS||19891008|F"
        expected_age = calculate_age(
            "19891008"
        )  # Assuming current date is fixed or calculate_age is mocked
        category, mrn, data = parse_system_message(message)
        self.assertEqual(category, "PAS-admit")
        self.assertEqual(mrn, "722269")
        self.assertEqual(data, [expected_age, "F"])

    def test_pas_discharge_message(self):
        # Mock a PAS discharge HL7 message
        message = "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240924153400||ADT^A03|||2.5\nPID|1||853518"
        category, mrn, data = parse_system_message(message)
        self.assertEqual(category, "PAS-discharge")
        self.assertEqual(mrn, "853518")
        self.assertEqual(data, ["", ""])

    def test_lims_message(self):
        # Mock a LIMS HL7 message
        message = "MSH|^~\&|SIMULATION|SOUTH RIVERSIDE|||20240924153600||ORU^R01|||2.5\nPID|1||54229\nOBR|1||||||20240924153600\nOBX|1|SN|CREATININE||103.56923163550283"
        category, mrn, data = parse_system_message(message)
        self.assertEqual(category, "LIMS")
        self.assertEqual(mrn, "54229")
        self.assertTrue(
            isinstance(data[1], float)
        )  # Ensure that the creatinine value is a float

    def test_incomplete_message(self):
        # Mock an incomplete HL7 message
        message = "MSH|...|..."
        with self.assertRaises(
            IndexError
        ):  # Assuming your function raises IndexError for incomplete messages
            parse_system_message(message)

    def test_strip_url_with_port(self):
        # Test stripping URL with port specified.
        url = "http://example.com:8080/path/to/resource"
        expected = ("example.com", 8080)
        self.assertEqual(strip_url(url), expected)

    def test_strip_url_without_port(self):
        # Test stripping URL without port, expecting None.
        url = "https://example.com/path/to/resource"
        expected = ("example.com", None)
        self.assertEqual(strip_url(url), expected)

    @patch("utils.socket")
    def test_read_success(self, mock_socket):
        # Mock socket to simulate successful read ending with MLLP_END_OF_BLOCK
        mock_socket.recv.side_effect = [b"Hello, World", MLLP_END_OF_BLOCK]
        sock = mock_socket()

        result, needs_reconnection = read_from_mllp(sock)
        self.assertEqual(result, b"Hello, World" + MLLP_END_OF_BLOCK)
        self.assertFalse(needs_reconnection)

    @patch("utils.socket")
    def test_connection_reset_error(self, mock_socket):
        # Mock socket to raise ConnectionResetError on recv
        mock_socket.recv.side_effect = ConnectionResetError("Connection reset by peer")
        sock = mock_socket()

        result, needs_reconnection = read_from_mllp(sock)
        self.assertIsNone(result)
        self.assertTrue(needs_reconnection)

    @patch("utils.socket")
    def test_generic_exception(self, mock_socket):
        # Mock socket to raise a generic exception on recv
        mock_socket.recv.side_effect = Exception("Generic error")
        sock = mock_socket()

        result, needs_reconnection = read_from_mllp(sock)
        self.assertIsNone(result)
        self.assertFalse(needs_reconnection)

    @patch("utils.datetime")
    def test_calculate_age(self, mock_datetime):
        # Set the current date to May 21, 2021 for consistent testing
        mock_datetime.now.return_value = datetime.datetime(2021, 5, 21)
        mock_datetime.datetime = datetime.datetime

        # Test cases
        test_cases = [
            ("19890521", 32),  # Person with birthday earlier in the year
            ("19890522", 31),  # Person with birthday on the test date
            ("20001231", 20),  # Person with birthday later in the year
        ]

        for dob, expected_age in test_cases:
            with self.subTest(dob=dob):
                self.assertEqual(calculate_age(dob), expected_age)


if __name__ == "__main__":
    unittest.main()
