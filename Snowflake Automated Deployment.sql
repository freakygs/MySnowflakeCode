-- DEV INSTANCE 

CREATE TABLE <DB_name>.<Schema_name>.CODE_DEPLOYMENT
(   
    UNIQUE_ID INTEGER NOT NULL DEFAULT date_part(epoch_second, current_timestamp()),
    DATABASE_NAME VARCHAR(100) NOT NULL DEFAULT '<DB_name>'
    SCHEMA_NAME VARCHAR(100) NOT NULL,
    OBJECT_NAME VARCHAR(100) NOT NULL,
    OBJECT_TYPE VARCHAR(15) NOT NULL,
    CR_NO VARCHAR(20) NOT NULL,
    DEPLOYMENT_STATUS VARCHAR(20) DEFAULT 'Scheduled',
    DEPLOYMENT_MESSAGE VARCHAR(1000) DEFAULT NULL,
    INSERT_USER VARCHAR(200) DEFAULT CURRENT_USER(),
    INSERT_TIMESTAMP TIMESTAMP_NTZ(0) DEFAULT CURRENT_TIMESTAMP(0)
);


-- share below table to PROD account
create TABLE <DB_name>.<Schema_name>.CODE_DEPLOYMENT_DDLS (
	UNIQUE_ID NUMBER(38,0) NOT NULL,
	DATABASE_NAME VARCHAR(100) NOT NULL COLLATE 'en-ci-trim',
	SCHEMA_NAME VARCHAR(100) NOT NULL COLLATE 'en-ci-trim',
	OBJECT_NAME VARCHAR(100) NOT NULL COLLATE 'en-ci-trim',
	ARGUMENT_SIGNATURE VARCHAR(500) COLLATE 'en-ci-trim',
	TARGET_DATABASE_NAME VARCHAR(100) NOT NULL COLLATE 'en-ci-trim',
	OBJECT_TYPE VARCHAR(15) NOT NULL COLLATE 'en-ci-trim',
	OBJECT_DDL VARCHAR(16777216) NOT NULL COLLATE 'en-cs-trim',
    INSERT_USER VARCHAR(200),
    INSERT_TIMESTAMP TIMESTAMP_NTZ(0)
);


CREATE TABLE <DB_name>.<Schema_name>.CODE_DEPLOYMENT_DATABASE_MAPPING
(
    SOURCE_DATABASE VARCHAR(40) NOT NULL,
    TARGET_DATABASE VARCHAR(40) NOT NULL
);



CREATE STREAM <DB_name>.<Schema_name>.STRM_DGTL_CODE_DEPLOYMENT_LOGS
ON TABLE <prod_db_share_name>.<Schema_name>.CODE_DEPLOYMENT_LOGS; 


create task <DB_name>.<Schema_name>.TASK_DGTL_CODE_DEPLOYMENT_UPDATE_STATUS
	schedule='15 MINUTE'
	when SYSTEM$STREAM_HAS_DATA('<Schema_name>.STRM_DGTL_CODE_DEPLOYMENT_LOGS')
	as EXECUTE IMMEDIATE
$$
BEGIN
    UPDATE <Schema_name>.CODE_DEPLOYMENT tgt 
		FROM <Schema_name>.STRM_DGTL_CODE_DEPLOYMENT_LOGS src
    SET tgt.DEPLOYMENT_STATUS = src.DEPLOYMENT_STATUS,
        tgt.DEPLOYMENT_MESSAGE = src.DEPLOYMENT_MESSAGE
    WHERE src.UNIQUE_ID = tgt.UNIQUE_ID
    AND src.DATABASE_NAME = tgt.DATABASE_NAME
    AND src.SCHEMA_NAME = tgt.SCHEMA_NAME
    AND src.OBJECT_NAME = tgt.OBJECT_NAME;
END;
$$;

alter task <DB_name>.<Schema_name>.TASK_DGTL_CODE_DEPLOYMENT_UPDATE_STATUS resume;

--------------------------------------- Above Script to be Implemented in Non Prod --------------------

--------------------------------------- Following Script to be Implemented in Prod --------------------
SELECT ABC from SNOWFLAKE;  -- Invalid code to cause failure

-- PROD INSTANCE
CREATE TABLE <DB_name>.<Schema_name>.CODE_DEPLOYMENT_DDLS_STAGE
(   
    UNIQUE_ID INTEGER NOT NULL,
    DATABASE_NAME VARCHAR(100) NOT NULL,
    SCHEMA_NAME VARCHAR(100) NOT NULL,
    OBJECT_NAME VARCHAR(100) NOT NULL,
    ARGUMENT_SIGNATURE VARCHAR(500),
    TARGET_DATABASE_NAME VARCHAR(100) NOT NULL,
    OBJECT_TYPE VARCHAR(15) NOT NULL,
    OBJECT_DDL VARCHAR NOT NULL,
    INSERT_USER VARCHAR(200),
    INSERT_TIMESTAMP TIMESTAMP_NTZ(0)
);


-- share below table to TEST account
create TABLE <DB_name>.<Schema_name>.CODE_DEPLOYMENT_LOGS (
	UNIQUE_ID NUMBER(38,0) NOT NULL,
	DATABASE_NAME VARCHAR(100) NOT NULL COLLATE 'en-ci-trim',
	SCHEMA_NAME VARCHAR(100) NOT NULL COLLATE 'en-ci-trim',
	OBJECT_NAME VARCHAR(100) NOT NULL COLLATE 'en-ci-trim',
	ARGUMENT_SIGNATURE VARCHAR(500) COLLATE 'en-ci-trim',
	TARGET_DATABASE_NAME VARCHAR(100) NOT NULL COLLATE 'en-ci-trim',
	OBJECT_TYPE VARCHAR(15) NOT NULL COLLATE 'en-ci-trim',
	DEPLOYMENT_STATUS VARCHAR(20) COLLATE 'en-ci-trim',
	DEPLOYMENT_MESSAGE VARCHAR(16777216) COLLATE 'en-ci-trim',
    INSERT_USER VARCHAR(200),
    INSERT_TIMESTAMP TIMESTAMP_NTZ(0)
);


CREATE STREAM <DB_name>.<Schema_name>.STRM_DGTL_CODE_DEPLOYMENT_DDLS
ON TABLE <shared_db_name>.<Schema_name>.CODE_DEPLOYMENT_DDLS; 


CREATE TASK <DB_name>.<Schema_name>.TASK_DGTL_CODE_DEPLOYMENT
SCHEDULE = '15 MINUTE'
-- WAREHOUSE = ''
WHEN SYSTEM$STREAM_HAS_DATA('<Schema_name>.STRM_DGTL_CODE_DEPLOYMENT_DDLS')
AS
CALL <Schema_name>.SP_CODE_DEPLOYMENT_DDL_EXECUTION(0);

alter task <DB_name>.<Schema_name>.TASK_DGTL_CODE_DEPLOYMENT resume ;



CREATE PROCEDURE <DB_name>.<Schema_name>.SP_CODE_DEPLOYMENT_DDL_EXECUTION("FLAG" NUMBER(38,0))
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE 

v_stream := ''<DB_name>.<Schema_name>.STRM_DGTL_CODE_DEPLOYMENT_DDLS'';
v_temp_table := ''<DB_name>.<Schema_name>.CODE_DEPLOYMENT_DDLS_STAGE'';
v_log_table := ''<DB_name>.<Schema_name>.CODE_DEPLOYMENT_LOGS'';

v_unique_id VARCHAR;
v_database_name VARCHAR;
v_schema_name VARCHAR;
v_object_name VARCHAR;
v_object_type VARCHAR;
v_target_database_name VARCHAR;
v_obj_ddl VARCHAR;
v_arg_signature VARCHAR;
v_insert_user VARCHAR;
v_insert_timestamp TIMESTAMP;
v_bkp_sch_name VARCHAR := ''EDLPRODFIX''; 
v_success_count INTEGER := 0;
v_failed_count INTEGER := 0;

BEGIN

    IF (FLAG = 0) THEN -- if called by task then truncate and load table from stream
        DELETE FROM IDENTIFIER(:v_temp_table);
        INSERT INTO IDENTIFIER(:v_temp_table) SELECT 
            UNIQUE_ID, DATABASE_NAME, SCHEMA_NAME, OBJECT_NAME, 
            ARGUMENT_SIGNATURE, TARGET_DATABASE_NAME, OBJECT_TYPE, OBJECT_DDL, INSERT_USER, INSERT_TIMESTAMP
        FROM IDENTIFIER(:v_stream)
        WHERE METADATA$ACTION = ''INSERT'';
    END IF;

    LET v_curr_objects RESULTSET:= (
        SELECT * FROM IDENTIFIER(:v_temp_table)
        ORDER BY 
            CASE 
                WHEN OBJECT_TYPE = ''TABLE'' THEN 1
                WHEN OBJECT_TYPE = ''VIEW'' THEN 2
                WHEN OBJECT_TYPE = ''PROCEDURE'' THEN 3
            ELSE
                4
            END
        );

    FOR rec IN v_curr_objects DO
        v_unique_id := rec.UNIQUE_ID;
        v_database_name := rec.DATABASE_NAME;
        v_schema_name := rec.SCHEMA_NAME;
        v_object_name := rec.OBJECT_NAME;
        v_arg_signature := rec.ARGUMENT_SIGNATURE;
        v_object_type := rec.OBJECT_TYPE;
        v_obj_ddl := rec.OBJECT_DDL;
        v_target_database_name := rec.TARGET_DATABASE_NAME;
        v_insert_user := rec.INSERT_USER;
		v_insert_timestamp := rec.INSERT_TIMESTAMP;

        LET v_table_present := 0;
        
        -- check if table already exists
        IF (v_object_type = ''TABLE'')    THEN 
            LET v_info_schema_view := v_target_database_name || ''.INFORMATION_SCHEMA.TABLES'';
            SELECT 1 INTO :v_table_present FROM IDENTIFIER(:v_info_schema_view)
            WHERE TABLE_TYPE = ''BASE TABLE''
            AND TABLE_SCHEMA = :v_schema_name
            AND TABLE_NAME = :v_object_name;
            
        END IF;

        -- rename/backup existing object if present 
        LET v_suffix := ''_BKP_'' || TO_CHAR(CURRENT_TIMESTAMP(), ''DDMMYYYY_hhmi'');
        LET v_orig_obj_name := v_target_database_name || ''.'' || v_schema_name || ''.'' || v_object_name;
        LET v_bkp_obj_name := v_target_database_name || ''.'' || v_bkp_sch_name || ''.'' || v_object_name || v_suffix;
        LET v_bkp_sql := ''ALTER '' || v_object_type || '' IF EXISTS '' || v_orig_obj_name || v_arg_signature || '' RENAME TO '' || v_bkp_obj_name || '';'';
		
		BEGIN
			EXECUTE IMMEDIATE :v_bkp_sql;
		EXCEPTION
            WHEN other THEN 
                LET v_msg := ''Failed to create backup object. \\nSQL : '' ||  v_bkp_sql ||  ''\\n'' || SQLCODE || '': '' || SQLERRM;
				
                -- insert failure message in log table 
                INSERT INTO IDENTIFIER(:v_log_table) VALUES (:v_unique_id, :v_database_name, :v_schema_name, :v_object_name, :v_arg_signature, :v_target_database_name, :v_object_type, ''Failed'', :v_msg, :v_insert_user, :v_insert_timestamp);
				v_failed_count := v_failed_count + 1;
				CONTINUE;  
                 
		END;


        -- execute new code ddl
        BEGIN
            EXECUTE IMMEDIATE :v_obj_ddl;

            INSERT INTO IDENTIFIER(:v_log_table) VALUES (:v_unique_id, :v_database_name, :v_schema_name, :v_object_name, :v_arg_signature, :v_target_database_name, :v_object_type, ''Deployed'', ''Success'',  :v_insert_user, :v_insert_timestamp);

            -- drop backup object when object is not table / view
            IF (v_object_type NOT IN (''TABLE'', ''VIEW''))   THEN
                LET v_drop_sql := ''DROP '' || v_object_type || '' IF EXISTS '' || v_bkp_obj_name || v_arg_signature;
                EXECUTE IMMEDIATE :v_drop_sql;
                
            END IF;
			
			v_success_count := v_success_count + 1;
            
        EXCEPTION
            WHEN other THEN 
                LET v_msg := SQLCODE || '': '' || SQLERRM;
                
                -- insert failure message in log table 
                INSERT INTO IDENTIFIER(:v_log_table) VALUES (:v_unique_id, :v_database_name, :v_schema_name, :v_object_name, :v_arg_signature, :v_target_database_name, :v_object_type, ''Failed'', :v_msg, :v_insert_user, :v_insert_timestamp);
                
                -- rollback 
                LET v_rollback_sql := ''ALTER '' || v_object_type || '' IF EXISTS '' || v_bkp_obj_name || v_arg_signature || '' RENAME TO '' || v_orig_obj_name;
                EXECUTE IMMEDIATE :v_rollback_sql;
				
				v_failed_count := v_failed_count + 1;
				
				CONTINUE; 
                
        END;

        -- if table already existed then copy data from bkp table 
        IF (v_table_present = 1)    THEN 
        BEGIN
            LET v_columns := '''';
            LET v_info_schema_view := v_target_database_name || ''.INFORMATION_SCHEMA.COLUMNS'';
            LET v_bkp_table_name := v_object_name || v_suffix;

            SELECT LISTAGG(COLUMN_NAME, '','') INTO :v_columns
            FROM (
                SELECT COLUMN_NAME FROM IDENTIFIER(:v_info_schema_view)
                WHERE TABLE_SCHEMA = :v_bkp_sch_name
                AND TABLE_NAME = :v_bkp_table_name
                INTERSECT
                SELECT COLUMN_NAME FROM IDENTIFIER(:v_info_schema_view)
                WHERE TABLE_SCHEMA = :v_schema_name
                AND TABLE_NAME = :v_object_name
            );

            LET v_insert_sql := ''INSERT INTO '' || v_orig_obj_name || ''('' || v_columns || '') SELECT '' || v_columns || '' FROM '' || v_bkp_obj_name;
            
            EXECUTE IMMEDIATE :v_insert_sql;

            -- update log table with the message that data copy succeeded
            UPDATE IDENTIFIER(:v_log_table)
            SET DEPLOYMENT_MESSAGE = ''Success : Data also copied. Backup Table name is '' || :v_bkp_obj_name
            WHERE UNIQUE_ID = :v_unique_id
            AND DATABASE_NAME = :v_database_name
            AND SCHEMA_NAME = :v_schema_name
            AND OBJECT_NAME = :v_object_name;
              
        EXCEPTION

            WHEN OTHER THEN 
                LET v_msg := SQLCODE || '': '' || SQLERRM;

                -- update failure message in log table 
                UPDATE IDENTIFIER(:v_log_table)
                SET DEPLOYMENT_MESSAGE = ''Success : Data copy failed. Please perform manual copy. Backup Table name is '' || :v_bkp_obj_name
                WHERE UNIQUE_ID = :v_unique_id
                AND DATABASE_NAME = :v_database_name
                AND SCHEMA_NAME = :v_schema_name
                AND OBJECT_NAME = :v_object_name;
				
				-- shift the object to failure count because of data copy failure
				v_success_count := v_success_count - 1;
				v_failed_count := v_failed_count + 1;

        END;
        END IF;

    END FOR;
	
	LET v_total_count := v_success_count + v_failed_count;
    
    LET v_email_msg := ''Total Objects : '' || v_total || ''\nSuccess Objects : '' || v_success_count || ''\nFailed (or Partial Success) Objects : '' || v_failed_count;
	
	CALL SYSTEM$SEND_EMAIL(
		''my_email_int'',
		'''',
		''Deployment Process Completed'',
		:v_email_msg
	);

    RETURN v_email_msg;

END
';







