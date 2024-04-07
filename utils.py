import hl7
import pandas as pd
import numpy as np
import datetime
import joblib
import csv
import sys
from statistics import median
from constants import (
    MLLP_START_CHAR,
    MLLP_END_CHAR,
    REVERSE_LABELS_MAP,
    MLLP_END_OF_BLOCK,
    ON_DISK_PAGER_STACK_PATH,
)
import requests
import sys
import time
import socket
import pickle


def process_mllp_message(data):
    """
    Extracts the HL7 message from the MLLP data.
    """
    start_index = data.find(MLLP_START_CHAR)
    end_index = data.find(MLLP_END_CHAR)
    if start_index != -1 and end_index != -1:
        return data[start_index + 1 : end_index]
    return None


def parse_hl7_message(hl7_data):
    """
    Parses the HL7 message and returns the parsed message object.
    """
    hl7_string = hl7_data.decode("utf-8").replace("\r", "\n")
    message = hl7.parse(hl7_string)
    return message


def create_acknowledgement():
    """
    Creates an HL7 ACK message for the received message.
    """
    # Construct the ACK message based on the simulator's expectations
    ack_msg = f"MSH|^~\\&|||||{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}||ACK||P|2.5\rMSA|AA|\r"
    framed_ack = MLLP_START_CHAR + ack_msg.encode() + MLLP_END_CHAR
    return framed_ack


def predict_with_dt(dt_model, data):
    """
    Following data needs to be passed:
    [
        "age",
        "sex",
        "C1",
        "RV1",
        "RV1_ratio",
        "RV2",
        "RV2_ratio",
        "change_within_48hrs",
        "D"
    ]
    Predict with the DT Model on the data.
    Returns the predicted labels.
    """
    y_pred = dt_model.predict(data)

    # Map the predictions to labels
    labels = [REVERSE_LABELS_MAP[item] for item in y_pred]

    return labels


def predict_with_mlp(mlp_model, data):
    """
    Following data needs to be passed:
    [
        "RV1",
        "RV2",
        "RV1_ratio",
        "RV2_ratio",
        "D_value",
        "C_value"
    ]
    Predict with the DT Model on the data.
    Returns the predicted labels.
    """
    y_pred = mlp_model.predict(data)

    # Map the predictions to labels
    labels = [REVERSE_LABELS_MAP[item] for item in y_pred]

    return labels


def populate_test_results_table(db, path):
    """
    Reads in the patient test result history and populates the table.
    Args:
        - db {InMemoryDatabase}: the database object
        - path {str}: path to the data
    """
    with open(path, newline="") as f:
        rows = csv.reader(f)
        for i, row in enumerate(rows):
            # skip header
            if i == 0:
                continue

            # remove empty strings
            while row and row[-1] == "":
                row.pop()

            mrn = row[0]
            # for each date, result pair insert into the table
            for j in range(1, len(row), 2):
                date = row[j]
                result = float(row[j + 1])
                db.insert_test_result(mrn, date, result)


def populate_patients_table(db, path):
    """
    Reads in the processed history file and populates the patients table.
    Args:
        - db {InMemoryDatabase}: the database object
        - path {str}: path to the data
    """
    with open(path, newline="") as f:
        rows = csv.reader(f)
        for i, row in enumerate(rows):
            # skip header
            if i == 0:
                continue

            # get patient info
            mrn = row[1]
            age = row[2]
            sex = row[3]

            # insert into the table
            db.insert_patient(mrn, age, sex)


def parse_system_message(message):
    """
    Parses the HL7 message and returns components of respective message type: PAS, LIMS

    Args:
    - message: HL7 message object

    Returns:
    - The category of message, MRN, [AGE, SEX] if PAS category or [DATE_BLOOD_TEST, CREATININE_VALUE] if LIMS
    """
    mrn = 0
    category = ""
    data = [""] * 2
    segments = str(message).split("\n")
    if len(segments) < 4:
        parsed_seg = segments[1].split("|")
        if len(parsed_seg) > 4:
            mrn = parsed_seg[3]
            category = "PAS-admit"
            date_of_birth = parsed_seg[7]
            data[0] = calculate_age(date_of_birth)
            data[1] = parsed_seg[8][0]
        else:
            mrn = parsed_seg[3].replace("\r", "")
            category = "PAS-discharge"
    else:
        mrn = segments[1].split("|")[3]
        category = "LIMS"
        data[0] = segments[2].split("|")[7]  # date of blood test
        data[1] = float(segments[3].split("|")[5])

    return category, mrn, data


def calculate_age(date_of_birth):
    """
    Calculates the age of a person given their date of birth.

    Args:
    - date_of_birth (str): The person's date of birth in "YYYYMMDD" format.

    Returns:
    - int: The age of the person as an integer.
    """
    # Parse the date of birth string into a datetime object
    dob = datetime.datetime.strptime(date_of_birth, "%Y%m%d")

    # Get the current date
    current_date = datetime.datetime.now()

    # Calculate the difference between the current date and the date of birth
    age = (
        current_date.year
        - dob.year
        - ((current_date.month, current_date.day) < (dob.month, dob.day))
    )

    return age


def D_value_compute(creat_latest_result, d1, lis):
    """
    Computes the D value, a measure based on the difference creatinine result values.

    :param creat_latest_result: The latest creatinine result.
    :param d1: The date of the latest creatinine result.
    :param d2: The date of the previous creatinine result.
    :param id_max: The index of the latest result in the row.
    :param row: The row of data from the dataframe.
    :return: The computed D value.
    """
    d1 = datetime.datetime.strptime(d1, "%Y%m%d%H%M%S")
    if type(lis[-1][3]) != int:
        d2 = datetime.datetime.strptime(lis[-1][3], "%Y-%m-%d %H:%M:%S")
    else:
        d2 = datetime.datetime.strptime(str(lis[-1][3]), "%Y%m%d%H%M%S")
    # Calculating the date within 48 hours
    past_two_days = d1 - datetime.timedelta(days=2)
    prev_lis_values = []
    change = False
    for i in range(len(lis)):
        if type(lis[i][3]) != int:
            d_ = datetime.datetime.strptime(lis[i][3], "%Y-%m-%d %H:%M:%S")
        else:
            d_ = datetime.datetime.strptime(str(lis[i][3]), "%Y%m%d%H%M%S")
        if d_ <= past_two_days:
            prev_lis_values.append(lis[i][4])
    if len(prev_lis_values) > 1:
        change = True
    elif len(prev_lis_values) == 1:
        change = False
    if len(prev_lis_values) > 0:
        # Finding the minimum value in the last two days
        minimum_previous_value = min(prev_lis_values)
        diff_D = float(creat_latest_result) - float(minimum_previous_value)
        return diff_D, change
    else:
        return 0, change


def RV_compute(creat_latest_result, d1, lis):
    """
    Computes the RV value, a measure based on the ratio of creatinine results.

    :param d1: The date of the latest creatinine result.
    :param d2: The date of the previous creatinine result.
    :param id_max: The index of the latest result in the row.
    :param row: The row of data from the dataframe.
    :return: The computed RV value.
    """
    # Calculating the difference of days between the two latest tests
    d1 = datetime.datetime.strptime(d1, "%Y%m%d%H%M%S")
    if type(lis[-1][3]) != int:
        d2 = datetime.datetime.strptime(lis[-1][3], "%Y-%m-%d %H:%M:%S")
    else:
        d2 = datetime.datetime.strptime(str(lis[-1][3]), "%Y%m%d%H%M%S")
    diff = abs(((d2 - d1).seconds) / 86400 + (d2 - d1).days)
    # If difference in less than 7 days then use the minimum to compute the ratio
    if diff <= 7:
        C1 = float(creat_latest_result)
        minimum = float(min([float(lis[i][4]) for i in range(len(lis))]))
        assert C1 / minimum is not None, "The RV value is None"
        return (
            C1,
            minimum,
            C1 / minimum,
            0,
            0,
        )  # C1, RV1, RV1_ratio, RV2, RV2_ratio
    # Else use the median of test results
    elif diff <= 365:
        C1 = float(creat_latest_result)
        median_ = float(median([float(lis[i][4]) for i in range(len(lis))]))
        assert C1 / median_ is not None, "The RV value is None"
        return C1, 0, 0, median_, C1 / median_  # C1, RV1, RV1_ratio, RV2, RV2_ratio
    else:
        return 0


def label_encode(sex):
    """
    Uses a Label encoder to encode categorical data.

    :param column: The list of features to be encoded.
    :return: List of encoded features.
    """
    if sex == "M" or sex == "m":
        return 0
    elif sex == "F" or sex == "f":
        return 1


def send_pager_request(mrn, latest_creatine_date, pager_address, pager_stack):
    """
    Sends pager requests for a given MRN and attempts to send additional requests from a stack until a failure occurs or all are sent.

    Args:
    - mrn (str): The mrn to send.
    - latest_creatine_date (str): The latest date of creatine measurement associated with the MRN.
    - pager_address (str): The pager service address
    - pager_stack (list of tuples): A stack (list) of MRNs and their associated creatine dates to send.

    Returns:
    - pager_stack (list of tuples): Updated pager stack with any requests that failed to send.

    Note:
    - This function uses an exponential backoff strategy for retries upon failed requests.
    """
    print("Sending a page for mrn:", mrn)
    # Define the URL for the pager request.
    pager_host, pager_port = strip_url(pager_address)
    print("Pager host and port: ", pager_host, pager_port)

    url = f"http://{pager_host}:{pager_port}/page"
    headers = {"Content-Type": "text/plain"}

    def attempt_send_request(mrn, latest_creatine_date):
        # Convert the MRN to a string and encode it to bytes, as the body of the POST request.
        data = str(mrn) + "," + str(latest_creatine_date)
        data = data.encode("utf-8")
        retry_delay = 0.4  # 0.4 second retry delay so we meet latency
        retries = 0
        max_retries = 3
        is_success = False
        while retries < max_retries:
            # Send the POST request with the MRN as the body.
            response = requests.post(url, data=data, headers=headers)

            # Check the response status code and print appropriate message.
            if response.status_code == 200:
                print(f"Request successful, server responded: {response.text}")
                is_success = True
                break
            else:
                print(
                    f"Attempt {retries + 1}: Request failed, status code: {response.status_code}, message: {response.text}"
                )
                retries += 1
                print("Retrying in", retry_delay, "seconds...")
                retry_delay = retry_delay * retries
                time.sleep(retry_delay)
        return is_success

    # First try to send the current MRN
    print("Sending current MRN: ", mrn)
    if attempt_send_request(mrn, latest_creatine_date):
        # If successful, attempt to send all requests in the stack
        print("Trying to send remaining pages...")
        while len(pager_stack) != 0:
            next_mrn, creatine_date = pager_stack.pop()
            if not attempt_send_request(next_mrn, creatine_date):
                # If a request fails, stop and re-append remaining requests including the failed one
                pager_stack.append(next_mrn, creatine_date)  # Re-add the failed MRN
                break
    else:
        # If initial request fails, append it to the stack
        pager_stack.append((mrn, latest_creatine_date))
    if len(pager_stack) != 0:
        print("Current pager stack:", pager_stack)
    return pager_stack


def load_model(file_path):
    """
    Loads a machine learning model from a pickle file.

    :param file_path: The path of the file where the model is stored.
    :return: The loaded model or None if an error occurs.
    """
    try:
        if file_path.endswith(".joblib"):
            with open(file_path, "rb") as file:
                model = joblib.load(file)
        elif file_path.endswith(".pkl"):
            with open(file_path, "rb") as file:
                model = pickle.load(file)
        return model
    except FileNotFoundError:
        print("File not found.")
        return None
    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def strip_url(url):
    """
    Strips the URL and returns the host and port alone.
    """
    print("Parsing URL:", url)
    url = url.split("://")[-1]

    # Split the URL by "/" to separate the host and potentially the port
    parts = url.split("/")

    # Get the host part
    host = parts[0].strip()

    # Check if the port is specified
    port = None
    if ":" in host:
        host, port = host.split(":")
        port = int(port)
    return host, port


def define_graceful_shutdown(db, current_socket, pager_stack):
    """
    Returns a function to gracefully shutdown the application. It is a wrapper as the signal library expects only signum and frame as the arguemnts and this way we can persisting the database, close the socket, and save the pager stack.

    Args:
    - db: Database object with `persist_db` and `close` methods.
    - current_socket: Dictionary containing the socket object under "sock" key.
    - pager_stack: Data structure to be saved on disk.

    Returns:
    - A signal handler function for graceful shutdown.
    """

    def graceful_shutdown(signum, frame):
        print("Graceful shutdown procedure started.")
        db.persist_db()
        db.close()
        print("Database persisted.")
        current_socket["sock"].close()
        print("MLLP connection closed.")
        with open(ON_DISK_PAGER_STACK_PATH, "wb") as file:
            pickle.dump(pager_stack, file)
        sys.exit(0)

    return graceful_shutdown


def exponential_backoff_retry(func):
    """
    Wraps a function to automatically retry with exponential backoff upon failure.

    This decorator tries to execute the wrapped function, retrying with an increasing delay if it fails. The delay doubles with each attempt, starting from 1 second, up to a maximum of 10 minutes between retries.

    Args:
    - func: The function to be wrapped and retried.

    Returns:
    - The wrapper function that handles the retries.
    """

    def wrapper(*args, **kwargs):
        base_delay = 1  # in seconds
        attempt = 0
        threshold = 600
        while True:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                wait_time = base_delay * (2**attempt)  # Exponential backoff
                attempt += 1
                print(
                    f"Attempt {attempt}, failed. Error: {e}; retrying in {wait_time} seconds..."
                )
                wait_time = min(
                    threshold, wait_time
                )  # ensures that the wait time is max 10 mins
                time.sleep(wait_time)

    return wrapper


@exponential_backoff_retry
def connect_to_mllp(host, port):
    """
    Attempts to connect to an MLLP server at the given host and port, retrying with
    exponential backoff on failure.

    Args:
    - host: The hostname of the MLLP server.
    - port: The port number of the MLLP server.

    Returns:
    - A socket object connected to the MLLP server.

    Raises:
    - Socket exceptions may be raised and caught by the decorator for retry. The function
      itself will not explicitly handle these exceptions, leaving that to the retry logic.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, int(port)))
    print(f"Connected to MLLP on {host}:{port}")
    return sock


def read_from_mllp(sock):
    """
    This function continuously reads from the socket in chunks of 1024 bytes until the MLLP end-of-block marker is found in the buffer. If the connection is reset during reading, it attempts to gracefully handle the error by closing the socket and indicating a need for reconnection.

    Args:
    - sock: The socket object representing the MLLP connection.

    Returns:
    - A tuple containing the buffer read from the connection and a boolean flag. The flag is True if the connection was reset and needs reconnection, False otherwise. If an error occurs, returns None for the buffer and the appropriate flag.
    """
    try:
        buffer = b""
        while MLLP_END_OF_BLOCK not in buffer:
            data = sock.recv(1024)
            buffer += data
        return buffer, False
    except ConnectionResetError as e:
        print("Connection was reset, reconnecting...")
        sock.close()
        return None, True
    except Exception as e:
        print(f"Failed to read an MLLP message; error: {e}")
        return None, False
