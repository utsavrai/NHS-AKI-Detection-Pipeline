#!/usr/bin/env python3

import signal
import pickle
import argparse
import threading
from joblib import load
from utils import (
    process_mllp_message,
    parse_hl7_message,
    create_acknowledgement,
    parse_system_message,
    strip_url,
    define_graceful_shutdown,
)
from memory_db import InMemoryDatabase
from constants import (
    DT_MODEL_PATH,
    FEATURES_COLUMNS,
    ON_DISK_PAGER_STACK_PATH,
    MLP_MODEL_PATH,
    DEFAULT_AGE,
    DEFAULT_SEX,
)
from utils import (
    D_value_compute,
    RV_compute,
    predict_with_dt,
    label_encode,
    send_pager_request,
    connect_to_mllp,
    read_from_mllp,
    predict_with_mlp,
)
from prometheus_metrics import (
    start_metrics_server,
    increment_socket_connections,
    increment_message_counter,
    increment_patient_admit_counter,
    increment_patient_discharge,
    process_blood_test,
    increment_blood_test_counter,
    increment_aki_counter,
    calculate_positive_aki_rate,
    increment_failure_counter,
    calculate_latency_average,
    increment_latency_counter,
)
from datetime import datetime
import pandas as pd
import numpy as np
import os
import sys
import traceback
from prometheus_client import start_http_server, Summary, Counter, Gauge

REQUEST_TIME = Summary("request_processing_seconds", "Time spent processing request")
SOCKET_RECONNECTIONS_COUNTER = Gauge(
    "socket_reconnections_total", "Total number of socket reconnections made"
)
SOCKET_RECONNECTIONS_COUNTER.set(-1)
MESSAGE_COUNTER = Counter("total_messages", "Total number of messages received")
PATIENT_ADMIT_COUNTER = Counter(
    "total_admitted_patients", "Total number of admitted patients"
)
PATIENT_DISCHARGE_COUNTER = Counter(
    "total_discharged_patients", "Total number of discharged patients"
)
BLOOD_TEST_AVERAGE = Gauge("blood_test_average", "Average Value of blood test")
LATENCY_AVERAGE = Gauge("latency_average", "Average Value of latency")
FAILURE_COUNTER = Counter("total_failures", "Total number of failures occurred")
LATENCY_EXCEEDS_COUNTER = Counter(
    "latency_exceeds_3_seconds_total",
    "Counts how many times latency exceeded 3 seconds",
)
TOTAL_BLOOD_TESTS = Counter("total_blood_test", "Total number of blood tests received")
TOTAL_POSITIVE_AKI = Counter(
    "total_positive_akis", "Total number of positive AKI instances detected"
)
AKI_POSITIVE_RATE = Gauge("positive_AKI_rate", "Positive AKI rate")


def start_server(
    history_load_path, mllp_address, pager_address, pager_stack, debug=False
):
    """
    Starts the TCP server to listen for incoming MLLP messages on the specified port.
    """
    if debug:
        latencies = []  # to measure latency
        outputs = []  # to measure f3 score
        count = 0
    mllp_host, mllp_port = strip_url(mllp_address)

    # Initialise the in-memory database
    db = InMemoryDatabase(history_load_path)  # this also loads the previous history

    if db.database_loaded() == True:
        print("Database loaded correctly")
    else:
        print("Database not loaded properly")

    assert db != None, "In-memory Database is not initialised properly..."
    # Variables to keep track of the total sum and count of blood test values
    total_blood_sum = 0.0
    count_blood = 0
    aki_count = 0
    latency_time = 0
    # Start the server
    sock = connect_to_mllp(mllp_host, mllp_port)
    increment_socket_connections(SOCKET_RECONNECTIONS_COUNTER)

    # store the current socket for connection management
    current_socket = {"sock": sock}

    # register signals for graceful shutdown
    signal.signal(
        signal.SIGINT, define_graceful_shutdown(db, current_socket, pager_stack)
    )
    signal.signal(
        signal.SIGTERM, define_graceful_shutdown(db, current_socket, pager_stack)
    )

    # Load the model once for use through out
    dt_model = load(DT_MODEL_PATH)
    assert dt_model != None, "Model is not loaded properly..."
    mlp_model = load(MLP_MODEL_PATH)
    assert mlp_model != None, "MLP Model is not loaded properly..."

    try:
        count_mlp = 0
        while True:
            data, need_to_reconnect = read_from_mllp(sock)

            if need_to_reconnect:
                sock = connect_to_mllp(mllp_host, mllp_port)
                # update the current socket for connection management - handle restart and reconnection
                current_socket["sock"] = sock
                increment_socket_connections(SOCKET_RECONNECTIONS_COUNTER)

            if data:
                hl7_data = process_mllp_message(data)
            else:
                hl7_data = None
                print("No data received.")

            if hl7_data:
                message = parse_hl7_message(hl7_data)

                category, mrn, data = parse_system_message(
                    message
                )  # category is type of system message and data consists of age sex if PAS admit or date of blood test and creatanine result
                print("Parsed values: ", category, mrn, data)
                increment_message_counter(MESSAGE_COUNTER)
                if category == "PAS-admit":
                    increment_patient_admit_counter(PATIENT_ADMIT_COUNTER)
                    # print('Patient {} inserted'.format(mrn))
                    print(f"PAS-Admit: Inserting {mrn} into db...")
                    db.insert_patient(mrn, int(data[0]), str(data[1]))
                    # check if patient was inserted correctly
                    if not db.get_patient(mrn):
                        print(f"Failed to insert patient {mrn}, trying once more")
                        # and try again
                        db.insert_patient(mrn, int(data[0]), str(data[1]))
                elif category == "PAS-discharge":
                    increment_patient_discharge(PATIENT_DISCHARGE_COUNTER)
                    print(f"PAS-discharge: Discharging {mrn} ...")
                    db.discharge_patient(mrn)
                    # check if patient was discharged correctly
                    if db.get_patient(mrn):
                        print(f"Failed to discharge patient {mrn}, trying once more")
                        # and try again
                        db.discharge_patient(mrn)
                elif category == "LIMS":
                    start_time = datetime.now()
                    print("Message from LIMS! Retreiving Patient History...")
                    patient_history = db.get_patient_history(str(mrn))

                    # prometheus related upates
                    total_blood_sum = total_blood_sum + data[1]
                    count_blood = count_blood + 1
                    process_blood_test(total_blood_sum, count_blood, BLOOD_TEST_AVERAGE)
                    increment_blood_test_counter(TOTAL_BLOOD_TESTS)

                    if len(patient_history) != 0:
                        print("Patient History found!")
                        if debug:
                            count = count + 1
                        latest_creatine_result = data[1]
                        latest_creatine_date = data[0]
                        D, change_ = D_value_compute(
                            latest_creatine_result,
                            latest_creatine_date,
                            patient_history,
                        )
                        C1, RV1, RV1_ratio, RV2, RV2_ratio = RV_compute(
                            latest_creatine_result,
                            latest_creatine_date,
                            patient_history,
                        )
                        features = [
                            patient_history[0][1],
                            label_encode(patient_history[0][2]),
                            C1,
                            RV1,
                            RV1_ratio,
                            RV2,
                            RV2_ratio,
                            change_,
                            D,
                        ]
                        print("Features created...")
                        input = pd.DataFrame([features], columns=FEATURES_COLUMNS)
                        print("Calling DT!")
                        aki = predict_with_dt(dt_model, input)
                    elif (
                        patient_history == None or len(patient_history) == 0
                    ) and db.get_patient(mrn):
                        print("Patient History doesn't exist...")
                        latest_creatine_result = data[1]
                        latest_creatine_date = data[0]
                        D = 0
                        change_ = 0
                        C1 = latest_creatine_result
                        RV1 = 0
                        RV1_ratio = 0
                        RV2 = 0
                        RV2_ratio = 0
                        features = [
                            db.get_patient(mrn)[1],
                            label_encode(db.get_patient(mrn)[2]),
                            C1,
                            RV1,
                            RV1_ratio,
                            RV2,
                            RV2_ratio,
                            change_,
                            D,
                        ]
                        print("Features created...")
                        input = pd.DataFrame([features], columns=FEATURES_COLUMNS)
                        print("Calling DT!")
                        aki = predict_with_dt(dt_model, input)

                    else:
                        # This ideally shouldn't happen -
                        count_mlp = count_mlp + 1
                        print(
                            "No such patient in the patients table. Inserting with default values..."
                        )

                        # insert the patient into the DB - with default values to avoid this flow the next time we get a test result for this patient
                        db.insert_patient(mrn, DEFAULT_AGE, DEFAULT_SEX)
                        print(f"Inserted new patient with MRN: {mrn}!")
                        # Predict NO AKI for the current LIMS message.
                        aki = ["n"]

                    # If predicted AKI, send the Pager request
                    # and update the pager stack
                    if aki[0] == "y":
                        pager_stack = send_pager_request(
                            mrn, latest_creatine_date, pager_address, pager_stack
                        )

                        if debug:
                            outputs.append((mrn, latest_creatine_date))

                        # prometheus related
                        increment_aki_counter(TOTAL_POSITIVE_AKI)
                        aki_count = aki_count + 1
                        calculate_positive_aki_rate(
                            count_blood, aki_count, AKI_POSITIVE_RATE
                        )

                    end_time = datetime.now()
                    latency = end_time - start_time
                    if latency.total_seconds() > 3:
                        increment_latency_counter(LATENCY_EXCEEDS_COUNTER)
                    latency_time = latency_time + latency.total_seconds()
                    calculate_latency_average(
                        latency_time, count_blood, LATENCY_AVERAGE
                    )
                    # insert the current test result into the DB
                    db.insert_test_result(mrn, data[0], data[1])

                    if debug:
                        latency = end_time - start_time
                        latencies.append(latency)

                    # check if test result was inserted correctly
                    if not db.get_test_result(mrn, data[0]):
                        print(
                            f"Failed to insert test result for {mrn} on {data[0]}, trying once more"
                        )
                        # and try again
                        db.insert_test_result(mrn, data[0], data[1])
                # after every message persist the data
                db.persist_db()
                # ack the message
                print("Sending ACK message...")
                ack_message = create_acknowledgement()
                sock.sendall(ack_message)
                print("-" * 80)
            else:
                print("No valid MLLP message received.")
    except Exception as e:
        increment_failure_counter(FAILURE_COUNTER)
        print("There was an exception in the main loop..")
        traceback.print_exc()
        sys.stderr.write(str(e))
    finally:
        # perform any cleanup or data persistance tasks
        # (this is done when we encounter an exception or if the
        # program finishes its flow normally - so it is separate from the
        # graceful shutdown)
        try:
            db.persist_db()
            db.close()
            print("Database persisted")
            print("Number of times MLP condition satisfied:", count_mlp)
        except:
            print("Database has already been persisted and closed.")

        try:
            current_socket["sock"].close()
            print("MLLP connection closed")
        except:
            print("MLLP connection has already been closed.")

        with open(ON_DISK_PAGER_STACK_PATH, "wb") as file:
            pickle.dump(pager_stack, file)

    if debug:
        print("Patients with Historical Data", count)

        # Calculate latency metrics
        mean_latency = np.mean(latencies)
        median_latency = np.median(latencies)
        min_latency = np.min(latencies)
        max_latency = np.max(latencies)
        percentile_99 = np.percentile(latencies, 99)

        metrics = {
            "Mean": mean_latency,
            "Median": median_latency,
            "Minimum": min_latency,
            "Maximum": max_latency,
            "99% Efficiency": percentile_99,
        }
        print(metrics)

        df = pd.DataFrame(outputs, columns=["mrn", "date"])
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d %H:%M:%S")
        df.to_csv("aki_predicted.csv", index=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--debug",
        default=False,
        type=bool,
        help="Whether to calculate F3 and Latency Score",
    )
    parser.add_argument(
        "--history",
        default="data/history.csv",
        type=str,
        help="Where to load the history.csv file from",
    )
    # Start the metrics server in a background thread
    metrics_thread = threading.Thread(target=start_metrics_server, args=(8000,))
    metrics_thread.daemon = True
    metrics_thread.start()
    HISTORY_PATH = os.environ.get("HISTORY_PATH", "data/history.csv")
    MLLP_LINK = os.environ.get("MLLP_ADDRESS", "0.0.0.0:8440")
    PAGER_LINK = os.environ.get("PAGER_ADDRESS", "0.0.0.0:8441")
    flags = parser.parse_args()
    pager_stack = []
    if os.path.exists(ON_DISK_PAGER_STACK_PATH):
        with open(ON_DISK_PAGER_STACK_PATH, "rb") as file:
            pager_stack = pickle.load(file)
    start_server(
        HISTORY_PATH, MLLP_LINK, PAGER_LINK, pager_stack=pager_stack, debug=flags.debug
    )


if __name__ == "__main__":
    main()
