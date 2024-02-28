import csv
import boto3
from pymongo import MongoClient
from io import StringIO

class MongoDBToMinIOCSVExporter:
    def __init__(self, mongodb_uri, database_name, collection_name, minio_endpoint, minio_access_key, minio_secret_key, minio_bucket_name, minio_object_key):
        self.mongodb_uri = mongodb_uri
        self.database_name = database_name
        self.collection_name = collection_name
        self.minio_endpoint = minio_endpoint
        self.minio_access_key = minio_access_key
        self.minio_secret_key = minio_secret_key
        self.minio_bucket_name = minio_bucket_name
        self.minio_object_key = minio_object_key

    def export_to_csv_in_minio(self):
        client = MongoClient(self.mongodb_uri)
        db = client[self.database_name]
        collection = db[self.collection_name]
        cursor = collection.find()

        csv_buffer = StringIO()
        fieldnames = [
            'patient_name', 'patient_age', 'patient_gender', 'patient_phone', 'patient_address',
            'conditions', 'allergies', 'medications_name', 'medications_dosage', 'medications_frequency',
            'appointments_date', 'appointments_doctor', 'appointments_reason'
        ]
        csv_writer = csv.DictWriter(csv_buffer, fieldnames=fieldnames)
        csv_writer.writeheader()

        for document in cursor:
            row = self.deserialize_document(document)
            csv_writer.writerow(row)

        # Connect to MinIO and upload the CSV file
        self.upload_to_minio(csv_buffer.getvalue())

    def deserialize_document(self, document):
        patient = document.get('patient', {})
        patient_info = patient.get('personal_information', {})
        medical_history = patient.get('medical_history', {})
        appointments = patient.get('appointments', [])

        row = {
            'patient_name': patient_info.get('name', ''),
            'patient_age': patient_info.get('age', ''),
            'patient_gender': patient_info.get('gender', ''),
            'patient_phone': '',
            'patient_address': '',
            'conditions': ', '.join(medical_history.get('conditions', [])),
            'allergies': ', '.join(medical_history.get('allergies', [])),
            'medications_name': ', '.join([med['name'] for med in medical_history.get('medications', [])]),
            'medications_dosage': ', '.join([med['dosage'] for med in medical_history.get('medications', [])]),
            'medications_frequency': ', '.join([med['frequency'] for med in medical_history.get('medications', [])])
        }

        # Check if contact details exist
        if 'contact_details' in patient_info:
            row['patient_phone'] = patient_info['contact_details'].get('phone', '')
            row['patient_address'] = patient_info['contact_details'].get('address', '')

        for appointment in appointments:
            row.update({
                'appointments_date': appointment.get('date', ''),
                'appointments_doctor': appointment.get('doctor', ''),
                'appointments_reason': appointment.get('reason', '')
            })

        return row

    def upload_to_minio(self, csv_data):
        csv_buffer = StringIO(csv_data)
        s3 = boto3.client(
            's3',
            endpoint_url=self.minio_endpoint,
            aws_access_key_id=self.minio_access_key,
            aws_secret_access_key=self.minio_secret_key,
            region_name='',  # region is not needed for MinIO
            config=boto3.session.Config(signature_version='s3v4')
        )

        csv_buffer.seek(0)
        s3.put_object(Bucket=self.minio_bucket_name, Key=self.minio_object_key, Body=csv_buffer.getvalue())
        print("CSV file uploaded to MinIO successfully.")


