from transform_and_load.db_connection import get_connection

conn = get_connection()
cursor = conn.cursor()

cursor.execute(f'''
  CREATE TABLE IF NOT EXISTS etl_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    job_id VARCHAR(255),
    stage VARCHAR(50),
    status ENUM('STARTED', 'SUCCESS', 'FAILED') NOT NULL,
    start_time DATETIME,
    end_time DATETIME,
    duration_seconds INT,
    records_processed INT,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
  );
''')

conn.commit()
cursor.close()
conn.close()