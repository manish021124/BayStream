from datetime import datetime
import uuid

class ETLLogger:
  def __init__(self, conn_or_callable, job_name):
    self.conn_provider = conn_or_callable if callable(conn_or_callable) else lambda: conn_or_callable
    self.job_name = job_name
    self.job_id = str(uuid.uuid4())
    self.stage_start_time = None
    self.stage_log_id = None
    self.current_stage = None

  def start(self, stage):
    self.stage_start_time = datetime.now()
    self.current_stage = stage
    conn = self.conn_provider()
    cursor = conn.cursor()
    try:
      cursor.execute("""
        INSERT INTO etl_log (job_id, stage, status, start_time)
        VALUES (%s, %s, %s, %s)
      """, (self.job_id, stage, 'STARTED', self.stage_start_time))
      conn.commit()
      self.stage_log_id = cursor.lastrowid
    finally:
      cursor.close()
      conn.close()

  def success(self, records_processed=0):
    end_time = datetime.now()
    duration = int((end_time - self.stage_start_time).total_seconds())
    conn = self.conn_provider()
    cursor = conn.cursor()
    try:
      cursor.execute("""
        UPDATE etl_log
        SET status=%s, end_time=%s, duration_seconds=%s, records_processed=%s
        WHERE id=%s
      """, ('SUCCESS', end_time, duration, records_processed, self.stage_log_id))
      conn.commit()
    finally:
      cursor.close()
      conn.close()

  def fail(self, error_msg):
    end_time = datetime.now()
    duration = int((end_time - self.stage_start_time).total_seconds())
    error_msg = (error_msg[:997] + '...') if len(error_msg) > 1000 else error_msg
    conn = self.conn_provider()
    cursor = conn.cursor()
    try:
      cursor.execute("""
        UPDATE etl_log
        SET status=%s, end_time=%s, duration_seconds=%s, error_message=%s
        WHERE id=%s
      """, ('FAILED', end_time, duration, error_msg, self.stage_log_id))
      conn.commit()
    finally:
      cursor.close()
      conn.close()
