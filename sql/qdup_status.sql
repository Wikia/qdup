DROP VIEW IF EXISTS qdup.qdup_status;

CREATE VIEW qdup.qdup_status AS
SELECT class,
       COUNT(1) AS jobs,
       COUNT(CASE WHEN status = 'ON HOLD'   THEN 1 ELSE null END) AS on_hold,
       COUNT(CASE WHEN status = 'WAITING'   THEN 1 ELSE null END) AS waiting,
       COUNT(CASE WHEN status = 'RUNNING'   THEN 1 ELSE null END) AS running,
       COUNT(CASE WHEN status = 'TIMEDOUT'  THEN 1 ELSE null END) AS timedout,
       COUNT(CASE WHEN status = 'FAILED'    THEN 1 ELSE null END) AS failed,
       COUNT(CASE WHEN status = 'COMPLETED' THEN 1 ELSE null END) AS completed,
       ROUND(100 * COUNT(CASE WHEN status = 'COMPLETED' THEN 1 ELSE null END) / COUNT(1), 2) AS percent_completed,
       COUNT(CASE WHEN status = 'COMPLETED' AND end_time > now() - INTERVAL 1 HOUR THEN 1 ELSE null END) AS last_hour,
       ROUND(60 * COUNT(CASE WHEN status = 'COMPLETED' AND end_time > now() - INTERVAL 1 HOUR THEN 1 ELSE null END) /
                  (UNIX_TIMESTAMP(now()) - UNIX_TIMESTAMP(MIN(CASE WHEN status = 'COMPLETED' AND end_time > now() - INTERVAL 1 HOUR THEN end_time ELSE null END))), 2) AS jobs_per_min
  FROM qdup.qdup_jobs
 GROUP BY class;



