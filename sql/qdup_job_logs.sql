CREATE TABLE qdup_job_logs (
    id            INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
    stdout        TEXT,
    stderr        TEXT,
    updated_at    TIMESTAMP,
    PRIMARY KEY (id)
) ENGINE=InnoDB;

