# Acute Kidney Injury Detection System

This repository hosts the design and implementation of an Acute Kidney Injury (AKI) detection system developed for South Riverside Hospital. The system employs a Decision Tree (DT) model to accurately detect AKI from patient data in real-time, significantly improving the speed and reliability of AKI diagnosis within the hospital.

## Objective

The primary objective is to create a robust AKI detection system that interfaces seamlessly with the hospital’s patient administration and laboratory information management systems, processing incoming patient data and issuing AKI alerts with high accuracy and minimal latency.

## Motivation

AKI is a critical condition that necessitates prompt detection and treatment. Manual detection methods are slow and can miss early signs of AKI. By automating the detection process with a more accurate machine learning model, we aim to facilitate timely medical intervention, thereby saving lives and reducing the hospital's burden.

## Goals

- **Real-time Data Processing:** Achieve a response time of under 3 seconds for processing and issuing alerts for 99% of cases.
- **High Prediction Accuracy:** Maintain a high level of prediction accuracy, aiming for an F3 score measurement.
- **Seamless Integration:** Ensure the system integrates with existing hospital data systems for real-time patient data processing.
- **Dynamic Data Handling:** Implement dynamic data storage with robust fault tolerance for active hospital patients and efficiently manage data post-patient discharge.

## Non-Goals

- Prediction of medical conditions other than AKI.
- Incremental model retraining.
- Overhaul of current hospital systems.

## User Story

A patient admitted to South Riverside Hospital undergoes routine blood tests. The AKI detection system, upon identifying potential AKI from the test results, promptly alerts the clinical response team for immediate intervention, showcasing the system's impact on patient care and recovery.

## Design Overview

The design includes:
- Patient identification upon arrival with record management in an in-memory database (Redis).
- Real-time processing of laboratory tests with updates managed by the laboratory information management system (LIMS).
- Continuous analysis of incoming patient data by the DT model to detect AKI markers.
- Notification of the healthcare team through the Pager Management System upon AKI detection.

## Alternatives Considered

While designing the system, alternatives around data storage and threading were evaluated. Redis was chosen for its advanced features like data persistence, replication, and atomic operations over simpler in-memory hash tables.

## Design Details

- **Patient Arrival and Registration:** New patients are registered with a unique MRN and added to the in-memory database, while returning patients have their records updated.
- **Laboratory Test Processing:** Test results are managed by LIMS and stored in Redis, serving as input for the AKI detection model.
- **Decision Tree Model:** The DT model analyzes patient data to identify AKI, leveraging the model’s interpretability and feature importance for accurate predictions.
- **Data Integration:** A combination of in-memory and persistent databases ensures fast data access and long-term data storage, adhering to regulatory standards.

## Testing

Comprehensive testing strategies including automated unit tests, integration tests, and benchmark tests are employed to ensure system reliability and performance meet clinical standards.

## Future Work

Potential enhancements include extending the system for detecting other critical conditions, streamlining clinical workflows, implementing online learning for continuous model improvement, and developing predictive models for hospital resource optimization.

## Contribution

Contributions are welcomed to further improve the system's functionality, efficiency, and to extend its capabilities to other critical healthcare applications.
