# DICOM Retriever

**DICOM Retriever** is a Python-based application for receiving and processing DICOM studies from connected MRI machines or other DICOM modalities. The application listens for DICOM files and handles the anonymization, logging, and upload of these files to Amazon S3. It comes with a **GUI** that allows technicians to configure the server settings and monitor ongoing server logs.

---

## Features

- **DICOM Server**: Accepts incoming DICOM studies on the specified IP and port.
- **GUI Interface**: User-friendly interface to configure the server IP, port, and AE Title.
- **Real-Time Logs**: Displays real-time server activity in the UI (e.g., study uploads, errors).
- **Anonymization**: Anonymizes patient data (e.g., names, birthdates) before uploading to S3.
- **S3 Integration**: Uploads processed DICOM files to Amazon S3 for storage.

---

## Requirements

To run the DICOM Retriever, you'll need the following Python packages:

- `pydicom` - Library for handling DICOM files.
- `pynetdicom` - A library for working with DICOM networks.
- `boto3` - AWS SDK for Python, used for interacting with Amazon S3.
- `python-dotenv` - To load environment variables from a `.env` file.
- `pylibjpeg`, `pylibjpeg-libjpeg`, `pylibjpeg-openjpeg`, `gdcm` - Libraries for image decoding and handling.

---

## Installation

Follow these steps to get your development environment set up:

### 1. Clone the repository:

```bash
git clone https://github.com/your-username/DICOM-Retriever.git
cd DICOM-Retriever
