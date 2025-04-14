import os
import logging
import asyncio
import boto3
import pydicom
import signal
import io
import sys
from pathlib import Path
from dotenv import load_dotenv
from pynetdicom import AE, evt, StoragePresentationContexts
from pynetdicom.sop_class import MRImageStorage, Verification
from logging.handlers import TimedRotatingFileHandler
from tracker_db import (
    init_db, get_pending_studies, get_study_files, upsert_study, 
    insert_file_record, mark_study_uploaded, cleanup_old_studies, 
    get_failed_uploads, update_file_upload_status, increment_retry_count,
    get_file_s3_links
)
import shutil
import traceback
import datetime
import re

# Configuration constants
INITIAL_RETRY_INTERVAL = 60  # Start with 1 minute
MAX_RETRY_INTERVAL = 300  # Maximum 5 minutes
CLEANUP_INTERVAL_SECONDS = 86400  # 24 hours between cleanups
CHECK_COMPLETION_INTERVAL_SECONDS = 60  # 1 minute between completion checks

# Initialize database
init_db()

class DICOMReceiver:
    def __init__(self, server_ip, server_port, local_ae_title):
        load_dotenv()
        self.server_ip = server_ip
        self.server_port = server_port
        self.local_ae_title = local_ae_title
        self.ae = AE(ae_title=local_ae_title)
        self._setup_logging()
        self.s3 = self._setup_s3_client()
        self._setup_client()
        self.received_studies_dir = Path("received_studies")
        self.received_studies_dir.mkdir(exist_ok=True)
        self.running = True
        self.background_tasks = []
        self.loop = None
        self.retry_intervals = {}  # Track retry intervals for each file
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _setup_logging(self):
        os.makedirs('logs', exist_ok=True)
        log_handler = TimedRotatingFileHandler("logs/retriever.log", when='midnight', backupCount=4, encoding='utf-8')
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        log_handler.setFormatter(formatter)
        self.logger = logging.getLogger('DICOMReceiver')
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(log_handler)
        # Add console handler for immediate feedback
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def _setup_client(self):
        """Set up DICOM client with supported presentation contexts."""
        # Add supported presentation contexts
        self.ae.supported_contexts = StoragePresentationContexts
        
        # Add requested contexts for verification
        transfer_syntaxes = [pydicom.uid.ImplicitVRLittleEndian, pydicom.uid.ExplicitVRLittleEndian]
        self.ae.add_requested_context(MRImageStorage, transfer_syntaxes)
        self.ae.add_requested_context(Verification, transfer_syntaxes)

    def _setup_s3_client(self):
        """Set up AWS S3 client with proper configuration."""
        aws_access_key = os.getenv('AWS_KEY')
        aws_secret_key = os.getenv('AWS_SECRET_KEY')
        aws_region = os.getenv('AWS_REGION', 'ap-south-1')
        
        if not aws_access_key or not aws_secret_key:
            self.logger.error("[ERROR] AWS credentials not found in environment variables")
            raise ValueError("AWS credentials not found in environment variables")
        
        try:
            s3_client = boto3.client('s3', 
                                aws_access_key_id=aws_access_key,
                                aws_secret_access_key=aws_secret_key, 
                                region_name=aws_region)
            
            # Test the connection and list buckets
            buckets = s3_client.list_buckets()
            self.logger.info("[S3] ====== AWS S3 Connection Test ======")
            self.logger.info(f"[S3] Successfully connected to AWS S3")
            self.logger.info(f"[S3] Available buckets: {[b['Name'] for b in buckets['Buckets']]}")
            self.logger.info(f"[S3] Region: {aws_region}")
            return s3_client
        except Exception as e:
            self.logger.error(f"[ERROR] Failed to initialize S3 client: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise

    def _signal_handler(self, sig, frame):
        """Handle termination signals for graceful shutdown."""
        self.logger.info(f"Received signal {sig}, initiating graceful shutdown...")
        self.running = False

    async def start(self):
        try:
            self.loop = asyncio.get_running_loop()
            handlers = [(evt.EVT_C_STORE, self.handle_store)]
            self.ae.start_server((self.server_ip, self.server_port), block=False, evt_handlers=handlers)
            self.logger.info(f"DICOM server started on {self.server_ip}:{self.server_port}")
            
            # Start background tasks
            self.background_tasks = [
                asyncio.create_task(self.check_study_completions()),
                asyncio.create_task(self.cleanup_old_studies_periodically()),
                asyncio.create_task(self.retry_failed_uploads())
            ]
            
            # Keep the server running
            while self.running:
                await asyncio.sleep(1)
                
            # Graceful shutdown
            self.logger.info("Shutting down DICOM server...")
            self.ae.shutdown()
            
            # Cancel all background tasks
            for task in self.background_tasks:
                if not task.done():
                    task.cancel()
                    try:
                        await task
                    except asyncio.CancelledError:
                        pass
            
            self.logger.info("DICOM server shutdown complete.")
        except Exception as e:
            self.logger.error(f"Error starting DICOM server: {str(e)}")
            self.logger.error(traceback.format_exc())
            raise

    def handle_store(self, event):
        """Handles incoming DICOM file storage, saves locally, and uploads to S3."""
        try:
            ds = event.dataset
            
            # Validate required DICOM tags
            if not self.validate_dicom_tags(ds):
                return 0xC000  # Failure
            
            patient_id = ds.PatientID
            study_uid = ds.StudyInstanceUID
            sop_instance_uid = ds.SOPInstanceUID
            
            # Create study folder in received_studies/
            study_folder = self.received_studies_dir / patient_id / study_uid
            study_folder.mkdir(parents=True, exist_ok=True)
            
            # Save DICOM file locally
            file_path = study_folder / f"{sop_instance_uid}.dcm"
            ds.save_as(file_path, write_like_original=True)
            self.logger.info(f"[FILE] DICOM file saved locally: {file_path}")
            self.logger.info(f"[INFO] Study folder contents: {list(study_folder.glob('*.dcm'))}")

            # Insert into database
            study_id = upsert_study(patient_id, study_uid)
            file_id = insert_file_record(study_id, str(file_path))
            
            # Schedule upload using the stored event loop
            if self.loop:
                self.loop.create_task(self._schedule_upload(file_path, ds, file_id))
            else:
                self.logger.error("No event loop available for scheduling upload")
            
            return 0x0000  # Success
        except Exception as e:
            self.logger.error(f"Error handling DICOM store: {str(e)}")
            self.logger.error(traceback.format_exc())
            return 0xC000  # Failure

    async def _schedule_upload(self, file_path, ds, file_id):
        """Schedule the upload in a separate thread to avoid blocking."""
        try:
            # Run the upload in a thread pool to avoid blocking
            await asyncio.to_thread(self._upload_and_cleanup, file_path, ds, file_id)
        except Exception as e:
            self.logger.error(f"Error scheduling upload: {str(e)}")
            self.logger.error(traceback.format_exc())
            update_file_upload_status(file_id, None, "failed")

    
    def _upload_buffer_to_s3(self, buffer, ds):
        bucket_name = os.getenv('AWS_BUCKET_NAME')
        if not bucket_name:
            self.logger.error("[ERROR] AWS_BUCKET_NAME not set")
            return None

        patient_id = ds.PatientID
        accession_number = getattr(ds, "AccessionNumber", "unknown")
        folder_path = f"{patient_id}/{patient_id}_anonymization/{accession_number}{patient_id}/"
        file_key = f"{folder_path}{ds.SOPInstanceUID}.dcm"
        s3_url = f"s3://{bucket_name}/{file_key}"

        try:
            self.logger.info(f"[S3] Uploading buffer to S3 at {s3_url}")
            self.s3.upload_fileobj(buffer, bucket_name, file_key)
            return s3_url
        except Exception as e:
            self.logger.error(f"[S3 ERROR] Upload failed: {str(e)}")
            self.logger.error(traceback.format_exc())
            return None

    def _upload_and_cleanup(self, file_path, ds, file_id):
        try:
            buffer, anon_ds = self.anonymize_dicom(file_path)
            if buffer and anon_ds:
                file_link = self._upload_buffer_to_s3(buffer, anon_ds)
                if file_link:
                    update_file_upload_status(file_id, file_link, "uploaded")
                    self.logger.info(f"[UPLOAD] Anonymized file uploaded to S3: {file_link}")
                    return True
                else:
                    update_file_upload_status(file_id, None, "failed")
                    return False
            else:
                update_file_upload_status(file_id, None, "failed")
                return False
        except Exception as e:
            self.logger.error(f"Upload/cleanup error: {str(e)}")
            self.logger.error(traceback.format_exc())
            update_file_upload_status(file_id, None, "failed")
            return False


    def anonymize_dicom(self, file_path):
        try:
            ds = pydicom.dcmread(file_path, force=True)
            for tag in ["PatientName", "PatientBirthDate", "PatientSex", "PatientAddress"]:
                if hasattr(ds, tag):
                    setattr(ds, tag, "")
            ds.remove_private_tags()

            output = io.BytesIO()
            ds.save_as(output, write_like_original=True)
            output.seek(0)
            return output, ds
        except Exception as e:
            self.logger.error(f"Error anonymizing DICOM file: {str(e)}")
            self.logger.error(traceback.format_exc())
            return None, None

    
    async def check_study_completions(self):
        """Checks if all study files have been uploaded."""
        while self.running:
            try:
                await asyncio.sleep(CHECK_COMPLETION_INTERVAL_SECONDS)
                for study in get_pending_studies():
                    files = get_study_files(study["study_id"])
                    if all(f["upload_status"] == "uploaded" for f in files):
                        mark_study_uploaded(study["study_id"])
                        self.logger.info(f"✅ Study {study['study_uid']} marked as uploaded.")
                        
                        # Log S3 links for verification
                        s3_links = get_file_s3_links()
                        study_files = [f for f in s3_links if f['study_uid'] == study['study_uid']]
                        self.logger.info(f"[S3] Study {study['study_uid']} S3 links:")
                        for file in study_files:
                            self.logger.info(f"[S3] - {file['original_file']} -> {file['s3_link']}")
            except asyncio.CancelledError:
                self.logger.info("Study completion checker task cancelled.")
                break
            except Exception as e:
                self.logger.error(f"Error checking study completions: {str(e)}")
                self.logger.error(traceback.format_exc())
                await asyncio.sleep(CHECK_COMPLETION_INTERVAL_SECONDS)  # Wait before retrying

    async def cleanup_old_studies_periodically(self):
        """Deletes old studies from the database and file system."""
        while self.running:
            try:
                # Log current studies before cleanup
                studies = list(self.received_studies_dir.glob('*/*'))
                self.logger.info(f"[INFO] Current studies before cleanup: {studies}")
                
                old_studies = cleanup_old_studies()
                for study in old_studies:
                    study_folder = self.received_studies_dir / study['patient_id'] / study['study_uid']
                    if study_folder.exists():
                        # Log study contents before deletion
                        files = list(study_folder.glob('*.dcm'))
                        self.logger.info(f"[INFO] Deleting study with {len(files)} files: {study_folder}")
                        shutil.rmtree(study_folder)
                        self.logger.info(f"[CLEANUP] Deleted old study files: {study_folder}")
                        patient_folder = study_folder.parent
                        if not any(patient_folder.iterdir()):
                            patient_folder.rmdir()
                            self.logger.info(f"[CLEANUP] Deleted empty patient folder: {patient_folder}")
            except asyncio.CancelledError:
                self.logger.info("Cleanup task cancelled.")
                break
            except Exception as e:
                self.logger.error(f"Error cleaning up old studies: {str(e)}")
                self.logger.error(traceback.format_exc())
            
            await asyncio.sleep(CLEANUP_INTERVAL_SECONDS)  # Runs every 24 hours

    async def retry_failed_uploads(self):
        """Retries failed S3 uploads with exponential backoff."""
        while self.running:
            try:
                failed_uploads = get_failed_uploads()
                if not failed_uploads:
                    await asyncio.sleep(INITIAL_RETRY_INTERVAL)
                    continue

                self.logger.info(f"[RETRY] Found {len(failed_uploads)} failed uploads to retry")
                
                for record in failed_uploads:
                    file_path = record["original_file"]
                    retry_count = record.get("retry_count", 0)
                    
                    if not os.path.exists(file_path):
                        self.logger.warning(f"[WARN] File no longer exists for retry: {file_path}")
                        continue
                    
                    # Calculate retry interval for this file
                    retry_interval = self._get_retry_interval(retry_count)
                    self.logger.info(f"[RETRY] Attempting upload for {file_path} (attempt {retry_count + 1}, interval: {retry_interval}s)")
                    
                    try:
                        ds = pydicom.dcmread(file_path, force=True)
                        
                        # Run the upload in a thread pool
                        def upload_with_result():
                            result = self._upload_and_cleanup(file_path, ds, record["id"])
                            return result
                        
                        # Run the upload and get the result
                        result = await asyncio.to_thread(upload_with_result)
                        
                        if result:
                            self.logger.info(f"[SUCCESS] Successfully uploaded file after {retry_count + 1} attempts: {file_path}")
                        else:
                            increment_retry_count(record["id"])
                            self.logger.warning(f"[RETRY] Upload failed for {file_path} (attempt {retry_count + 1})")
                            
                    except Exception as e:
                        self.logger.error(f"[ERROR] Error during retry upload: {str(e)}")
                        self.logger.error(traceback.format_exc())
                        increment_retry_count(record["id"])
                    
                    # Wait for the calculated interval before next retry
                    await asyncio.sleep(retry_interval)
                    
            except asyncio.CancelledError:
                self.logger.info("[INFO] Retry uploads task cancelled.")
                break
            except Exception as e:
                self.logger.error(f"[ERROR] Error in retry task: {str(e)}")
                self.logger.error(traceback.format_exc())
                await asyncio.sleep(INITIAL_RETRY_INTERVAL)

    def _get_retry_interval(self, retry_count):
        """Calculate retry interval with exponential backoff."""
        interval = min(INITIAL_RETRY_INTERVAL * (2 ** (retry_count // 3)), MAX_RETRY_INTERVAL)
        return interval

    def validate_dicom_tags(self, ds):
        """Validate DICOM tags according to DICOM standard."""
        required_tags = {
            'PatientID': {'type': str, 'min_length': 1},
            'StudyInstanceUID': {'type': str, 'pattern': r'^[0-9\.]+$'},
            'SOPInstanceUID': {'type': str, 'pattern': r'^[0-9\.]+$'}
        }
        
        for tag, requirements in required_tags.items():
            if tag not in ds:
                self.logger.error(f"Missing required DICOM tag: {tag}")
                return False
                
            value = ds[tag].value
            
            # Check type
            if not isinstance(value, requirements['type']):
                self.logger.error(f"Invalid type for {tag}: expected {requirements['type']}, got {type(value)}")
                return False
                
            # Check string length
            if 'min_length' in requirements and len(str(value)) < requirements['min_length']:
                self.logger.error(f"Invalid length for {tag}: {value}")
                return False
                
            # Check pattern
            if 'pattern' in requirements and not re.match(requirements['pattern'], str(value)):
                self.logger.error(f"Invalid format for {tag}: {value}")
                return False
                
        return True

async def main():
    try:
        # Load environment variables
        load_dotenv()
        
        # Get server configuration from environment variables with defaults
        server_ip = os.getenv('DICOM_SERVER_IP', '192.168.10.142')
        server_port = int(os.getenv('DICOM_SERVER_PORT', '11112'))
        local_ae_title = os.getenv('DICOM_AE_TITLE', 'STREAMSCP')
        
        # Log configuration
        logging.info(f"Starting DICOM server with configuration:")
        logging.info(f"  Server IP: {server_ip}")
        logging.info(f"  Server Port: {server_port}")
        logging.info(f"  AE Title: {local_ae_title}")
        
        # Create and start the DICOM receiver
        receiver = DICOMReceiver(server_ip=server_ip, server_port=server_port, local_ae_title=local_ae_title)
        await receiver.start()
    except Exception as e:
        logging.error(f"Fatal error in main: {str(e)}")
        logging.error(traceback.format_exc())
        raise

if __name__ == "__main__":
    asyncio.run(main())