CREATE TABLE qdup_jobs_new (
    id            INTEGER UNSIGNED NOT NULL AUTO_INCREMENT,
    queue         VARCHAR(255) NOT NULL DEFAULT 'main',
    class         VARCHAR(255) NOT NULL,
    status        ENUM('ON HOLD','WAITING','RUNNING','TIMEDOUT','FAILED','COMPLETED') DEFAULT 'WAITING',
    priority      TINYINT UNSIGNED DEFAULT 100,
    updated_at    TIMESTAMP,
    run_after     DATETIME DEFAULT '1970-01-01',
    begin_time    DATETIME,
    end_time      DATETIME,
    worker        VARCHAR(255) DEFAULT '',
    args          VARCHAR(2048) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL DEFAULT '',
--  <custom columns>
    PRIMARY KEY (id)
) ENGINE=InnoDB;
