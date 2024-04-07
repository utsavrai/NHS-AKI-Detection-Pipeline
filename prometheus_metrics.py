from prometheus_client import start_http_server, Summary, Counter

def start_metrics_server(port=8000):
    """
    Starts a background thread to serve Prometheus metrics.
    """
    start_http_server(port)

def increment_socket_connections(SOCKET_CONNECTIONS_COUNTER):
    """
    Increments the Socket connections counter.
    """
    SOCKET_CONNECTIONS_COUNTER.inc()

def increment_message_counter(MESSAGE_COUNTER):
    """
    Increments the Message counter.
    """
    MESSAGE_COUNTER.inc()

def increment_patient_admit_counter(PATIENT_ADMIT_COUNTER):
    """
    Increments the Patient admit counter.
    """
    PATIENT_ADMIT_COUNTER.inc()

def increment_patient_discharge(PATIENT_DISCHARGE_COUNTER):
    """
    Increments the Patient discharge counter.
    """
    PATIENT_DISCHARGE_COUNTER.inc()

def process_blood_test(total_sum, count, blood_test_gauge):
    """
    Simulate processing the blood test and set the average value in the gauge.
    """
    running_average = total_sum / count
    blood_test_gauge.set(running_average)

def increment_failure_counter(FAILURE_COUNTER):
    """
    Increments the Failure counter.
    """
    FAILURE_COUNTER.inc()

def increment_blood_test_counter(TOTAL_BLOOD_TESTS):
    """
    Increments the total number of Blood tests counter.
    """
    TOTAL_BLOOD_TESTS.inc()

def increment_aki_counter(TOTAL_POSITIVE_AKI):
    """
    Increments the total number of positive AKI instances.
    """
    TOTAL_POSITIVE_AKI.inc()

def calculate_positive_aki_rate(total_messages, positive_aki, aki_positive_gauge):
    """
    Calculates the positive AKI rate
    """
    rate = positive_aki / total_messages
    aki_positive_gauge.set(rate)

def calculate_latency_average(total_sum, count_blood, latency_gauge):
    """
    Calculates the average latency
    """
    rate = total_sum / count_blood
    latency_gauge.set(rate)

def increment_latency_counter(LATENCY_MISS_COUNTER):
    """
    Increments the total number of instances where latency was greater than 3s.
    """
    LATENCY_MISS_COUNTER.inc()