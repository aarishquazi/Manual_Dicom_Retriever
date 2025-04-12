import sqlite3
import datetime
import logging
import os
from contextlib import contextmanager

DB_PATH = "tracker.db"

# Set up logging
logger = logging.getLogger('DICOMReceiver')

@contextmanager
def get_db_connection():
    """Context manager for database connections."""
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row  # Enable row factory for named columns
        yield conn
    except sqlite3.Error as e:
        logger.error(f"Database error: {str(e)}")
        raise
    finally:
        if conn:
            conn.close()

def init_db():
    """Initialize the SQLite database and create tables if they do not exist."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS study (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    patient_id TEXT NOT NULL,
                    study_uid TEXT NOT NULL UNIQUE,
                    uploaded BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            cursor.execute("""
                CREATE TABLE IF NOT EXISTS file_records (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    study_id INTEGER NOT NULL,
                    original_file TEXT NOT NULL,
                    s3_link TEXT,
                    upload_status TEXT DEFAULT 'pending',
                    retry_count INTEGER DEFAULT 0,
                    FOREIGN KEY (study_id) REFERENCES study (id) ON DELETE CASCADE
                )
            """)

            conn.commit()
            logger.info("✅ Database initialized.")
    except Exception as e:
        logger.error(f"Error initializing database: {str(e)}")
        raise

def validate_inputs(**kwargs):
    """Validate input parameters."""
    for key, value in kwargs.items():
        if value is None:
            raise ValueError(f"{key} cannot be None")
        if isinstance(value, str) and not value.strip():
            raise ValueError(f"{key} cannot be empty")

def upsert_study(patient_id, study_uid):
    """Insert a new study or return existing ID if already present."""
    validate_inputs(patient_id=patient_id, study_uid=study_uid)
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO study (patient_id, study_uid) 
                VALUES (?, ?) 
                ON CONFLICT(study_uid) DO NOTHING
            """, (patient_id, study_uid))

            conn.commit()

            cursor.execute("SELECT id FROM study WHERE study_uid = ?", (study_uid,))
            result = cursor.fetchone()
            
            if result is None:
                raise ValueError(f"Failed to retrieve study ID for {study_uid}")
                
            return result['id']
    except Exception as e:
        logger.error(f"Error in upsert_study: {str(e)}")
        raise

def insert_file_record(study_id, original_file, s3_link=None, upload_status="pending"):
    """Insert a new file record associated with a study."""
    validate_inputs(study_id=study_id, original_file=original_file)
    
    if not os.path.exists(original_file):
        raise FileNotFoundError(f"File not found: {original_file}")
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                INSERT INTO file_records (study_id, original_file, s3_link, upload_status)
                VALUES (?, ?, ?, ?)
            """, (study_id, original_file, s3_link, upload_status))

            conn.commit()
            return cursor.lastrowid
    except Exception as e:
        logger.error(f"Error in insert_file_record: {str(e)}")
        raise

def get_pending_studies():
    """Retrieve studies that are not marked as uploaded yet."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute("SELECT id, patient_id, study_uid FROM study WHERE uploaded = 0")
            
            studies = [{"study_id": row['id'], 
                       "patient_id": row['patient_id'], 
                       "study_uid": row['study_uid']} 
                      for row in cursor.fetchall()]
            
            return studies
    except Exception as e:
        logger.error(f"Error in get_pending_studies: {str(e)}")
        raise

def get_study_files(study_id):
    """Retrieve all file records for a given study ID."""
    validate_inputs(study_id=study_id)
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT id, original_file, s3_link, upload_status 
                FROM file_records 
                WHERE study_id = ?
            """, (study_id,))

            files = [{"id": row['id'], 
                     "original_file": row['original_file'], 
                     "s3_link": row['s3_link'], 
                     "upload_status": row['upload_status']} 
                    for row in cursor.fetchall()]
            
            return files
    except Exception as e:
        logger.error(f"Error in get_study_files: {str(e)}")
        raise

def mark_study_uploaded(study_id):
    """Mark a study as fully uploaded to S3."""
    validate_inputs(study_id=study_id)
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("UPDATE study SET uploaded = 1 WHERE id = ?", (study_id,))
            
            if cursor.rowcount == 0:
                logger.warning(f"No study found with ID {study_id}")
            
            conn.commit()
    except Exception as e:
        logger.error(f"Error in mark_study_uploaded: {str(e)}")
        raise

def cleanup_old_studies():
    """Delete studies older than 2 days to free up space in both DB and file system."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            two_days_ago = datetime.datetime.now() - datetime.timedelta(days=2)
            
            # Get list of old studies before deletion
            cursor.execute("""
                SELECT patient_id, study_uid FROM study WHERE created_at < ?
            """, (two_days_ago.strftime('%Y-%m-%d %H:%M:%S'),))
            
            old_studies = [{"patient_id": row['patient_id'], 
                           "study_uid": row['study_uid']} 
                          for row in cursor.fetchall()]

            # Delete from database
            cursor.execute("""
                DELETE FROM file_records 
                WHERE study_id IN (SELECT id FROM study WHERE created_at < ?)
            """, (two_days_ago.strftime('%Y-%m-%d %H:%M:%S'),))
            
            cursor.execute("""
                DELETE FROM study 
                WHERE created_at < ?
            """, (two_days_ago.strftime('%Y-%m-%d %H:%M:%S'),))

            conn.commit()
            
            return old_studies  # Return list of deleted studies for file system cleanup
    except Exception as e:
        logger.error(f"Error in cleanup_old_studies: {str(e)}")
        raise

def get_failed_uploads():
    """Retrieve file records that failed to upload."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                SELECT id, original_file, retry_count 
                FROM file_records 
                WHERE upload_status = 'failed'
            """)
            
            failed_files = [{"id": row['id'], 
                            "original_file": row['original_file'],
                            "retry_count": row['retry_count']} 
                           for row in cursor.fetchall()]
            
            return failed_files
    except Exception as e:
        logger.error(f"Error in get_failed_uploads: {str(e)}")
        raise

def get_file_s3_links():
    """Get all file records with their S3 links."""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT f.id, f.original_file, f.s3_link, f.upload_status, 
                       s.patient_id, s.study_uid
                FROM file_records f
                JOIN study s ON f.study_id = s.id
                ORDER BY f.id DESC
            """)
            return [dict(row) for row in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Error getting S3 links: {str(e)}")
        raise

def update_file_upload_status(file_id, s3_link, status):
    """Update file upload status in the database."""
    validate_inputs(file_id=file_id, status=status)
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            # Log the update
            logger.info(f"[DB] Updating file {file_id} with S3 link: {s3_link}, status: {status}")
            
            cursor.execute("""
                UPDATE file_records 
                SET s3_link = ?, upload_status = ? 
                WHERE id = ?
            """, (s3_link, status, file_id))
            
            if cursor.rowcount == 0:
                logger.warning(f"No file record found with ID {file_id}")
            else:
                logger.info(f"[DB] Successfully updated file {file_id}")
            
            conn.commit()
    except Exception as e:
        logger.error(f"Error in update_file_upload_status: {str(e)}")
        raise

def increment_retry_count(file_id):
    """Increment the retry count for a file record."""
    validate_inputs(file_id=file_id)
    
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("""
                UPDATE file_records 
                SET retry_count = retry_count + 1 
                WHERE id = ?
            """, (file_id,))
            
            if cursor.rowcount == 0:
                logger.warning(f"No file record found with ID {file_id}")

            conn.commit()
    except Exception as e:
        logger.error(f"Error in increment_retry_count: {str(e)}")
        raise

