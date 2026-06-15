CREATE TABLE qbl.mq_pkg_log (
    mq_log_id               NUMBER 
                            GENERATED ALWAYS AS 
                            IDENTITY (START WITH 1000 INCREMENT BY 1),
    mq_id                   NUMBER,
    json_record             CLOB DEFAULT ON NULL EMPTY_CLOB(),
    error_code              NUMBER,
    error_message           VARCHAR2(3600 CHAR),
    error_location          VARCHAR2(800 CHAR),
    mq_status_code          NUMBER DEFAULT 500,
    procedure_name          VARCHAR2(32 CHAR),
    procedure_description   VARCHAR2(480 CHAR),
    package_name            VARCHAR2(64 CHAR),
    updated_at              TIMESTAMP 
                            DEFAULT ON NULL SYSTIMESTAMP,
    updated_by              VARCHAR2(32 CHAR) 
                            DEFAULT ON NULL NVL(SYS_CONTEXT('userenv','client_identifier'),USER),
    created_at              TIMESTAMP 
                            DEFAULT ON NULL SYSTIMESTAMP,
    created_by              VARCHAR2(32 CHAR) 
                            DEFAULT ON NULL NVL(SYS_CONTEXT('userenv','client_identifier'),USER),

    /* Constraints */
    CONSTRAINT pk_mq_pkg_log PRIMARY KEY (mq_log_id),
    CONSTRAINT chk_mq_log_json CHECK (json_record IS JSON)
);
