CREATE TABLE tasks
(
    domain_id      BINARY(16)   NOT NULL,
    task_list_name VARCHAR(255) NOT NULL,
    task_type      TINYINT      NOT NULL, -- {Activity, Decision}
    task_id        BIGINT       NOT NULL,
    --
    data           MEDIUMBLOB   NOT NULL,
    data_encoding  VARCHAR(16)  NOT NULL,
    PRIMARY KEY (domain_id, task_list_name, task_type, task_id)
);

CREATE TABLE task_lists
(
    shard_id      INT          NOT NULL,
    domain_id     BINARY(16)   NOT NULL,
    name          VARCHAR(255) NOT NULL,
    task_type     TINYINT      NOT NULL, -- {Activity, Decision}
    --
    range_id      BIGINT       NOT NULL,
    data          MEDIUMBLOB   NOT NULL,
    data_encoding VARCHAR(16)  NOT NULL,
    PRIMARY KEY (shard_id, domain_id, name, task_type)
);

