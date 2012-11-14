DROP VIEW IF EXISTS qdup.qdup_status;

CREATE VIEW qdup.qdup_status AS
SELECT class,
       priority,
       COUNT(CASE WHEN status = 'ON HOLD'   THEN 1 ELSE null END) AS on_hold,
       COUNT(CASE WHEN status = 'WAITING'   THEN 1 ELSE null END) AS waiting,
       COUNT(CASE WHEN status = 'RUNNING'   THEN 1 ELSE null END) AS running,
       COUNT(CASE WHEN status = 'TIMEDOUT'  THEN 1 ELSE null END) AS timedout,
       COUNT(CASE WHEN status = 'FAILED'    THEN 1 ELSE null END) AS failed,
       COUNT(CASE WHEN status = 'COMPLETED' THEN 1 ELSE null END) AS completed,
       COUNT(1) AS total,
       ROUND(100 * COUNT(CASE WHEN status = 'COMPLETED' THEN 1 ELSE null END) / COUNT(1), 2) AS percent,
       COUNT(CASE WHEN status = 'COMPLETED'
                   AND begin_time > now() - INTERVAL 10 MINUTE
                   AND end_time   > now() - INTERVAL 10 MINUTE THEN 1 ELSE null END) AS last_10_min,
       ROUND(60 * COUNT(CASE WHEN status = 'COMPLETED'
                              AND begin_time > now() - INTERVAL 10 MINUTE
                              AND end_time   > now() - INTERVAL 10 MINUTE THEN 1 ELSE null END) /
                  (UNIX_TIMESTAMP(now()) - UNIX_TIMESTAMP(MIN(CASE WHEN status = 'COMPLETED'
                                                                    AND begin_time > now() - INTERVAL 10 MINUTE
                                                                    AND end_time   > now() - INTERVAL 10 MINUTE THEN begin_time ELSE null END))), 2) AS jobs_per_min
  FROM qdup.qdup_jobs
 GROUP BY class;



