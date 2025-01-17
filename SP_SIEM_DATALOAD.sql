
CREATE OR REPLACE PROCEDURE DBC.DBC.SP_SIEM_DATALOAD() 
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS 
$$
DECLARE 

    v_start_date                 VARCHAR;
    v_last_query_completed_time  TIMESTAMP;  
    SQL_statement                VARCHAR; 
    v_date_hour                  INTEGER;   
    v_start_date_hour            VARCHAR;
    v_account                    varchar(100);

BEGIN

    select current_account() INTO v_account;


    SELECT COALESCE(MAX(COMPLETED_TIME), DATE_TRUNC('month', current_timestamp())) 
	  INTO v_last_query_completed_time
      FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME=>'TASK_SP_SIEM_DATALOAD'))
     WHERE STATE = 'SUCCEEDED';

    v_start_date := to_varchar(v_last_query_completed_time::DATE, 'YYYYMMDD');
	v_start_date_hour := to_varchar(v_last_query_completed_time::TIMESTAMP, 'YYYYMMDDHH24MISS');
	v_date_hour := MINUTE(v_last_query_completed_time);

    SQL_statement := '
                       COPY INTO @DBC.DBC.STG_SNC_SIEM/'||:v_start_date||'/QUERY_HISTORY_'||:v_start_date_hour||' 
                       FROM ( SELECT OBJECT_CONSTRUCT(*) AS data 
					          FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
                              WHERE START_TIME >= CAST('''||:v_last_query_completed_time||''' AS TIMESTAMP)
                            )
							FILE_FORMAT=(TYPE=JSON COMPRESSION = NONE  NULL_IF=(''NULL'', ''null''))
                            OVERWRITE=TRUE MAX_FILE_SIZE = 3200000000;
                     ';	
	

	EXECUTE IMMEDIATE :SQL_statement;

    SQL_statement := '
                       COPY INTO @DBC.DBC.STG_SNC_SIEM/'||:v_start_date||'/LOGIN_HISTORY_'||:v_start_date_hour||' 
                       FROM ( SELECT OBJECT_CONSTRUCT(*) AS data 
					          FROM TABLE(INFORMATION_SCHEMA.LOGIN_HISTORY())
                              WHERE EVENT_TIMESTAMP >= CAST('''||:v_last_query_completed_time||''' AS TIMESTAMP)
                            )
							FILE_FORMAT=(TYPE=JSON COMPRESSION = NONE  NULL_IF=(''NULL'', ''null''))
                            OVERWRITE=TRUE MAX_FILE_SIZE = 3200000000;
                     ';	
	
	EXECUTE IMMEDIATE :SQL_statement;

    SQL_statement := '
                       COPY INTO @DBC.DBC.STG_SNC_SIEM/'||:v_start_date||'/ACCESS_HISTORY_'||:v_start_date_hour||' 
                       FROM ( SELECT OBJECT_CONSTRUCT(*) AS data 
					          FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY
                              WHERE QUERY_START_TIME >= CAST('''||:v_last_query_completed_time||''' AS TIMESTAMP)
                            )
							FILE_FORMAT=(TYPE=JSON COMPRESSION = NONE  NULL_IF=(''NULL'', ''null''))
                            OVERWRITE=TRUE MAX_FILE_SIZE = 3200000000;
                     ';	
	
	EXECUTE IMMEDIATE :SQL_statement;


    SQL_statement := '
                       COPY INTO @DBC.DBC.STG_SNC_SIEM/'||:v_start_date||'/COPY_HISTORY_'||:v_start_date_hour||' 
                       FROM ( SELECT OBJECT_CONSTRUCT(*) AS data 
					          FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
                              WHERE LAST_LOAD_TIME >= CAST('''||:v_last_query_completed_time||''' AS TIMESTAMP)
                            )
							FILE_FORMAT=(TYPE=JSON COMPRESSION = NONE  NULL_IF=(''NULL'', ''null''))
                            OVERWRITE=TRUE MAX_FILE_SIZE = 3200000000;
                     ';	
	
	EXECUTE IMMEDIATE :SQL_statement;
	

    SQL_statement := '
                       COPY INTO @DBC.DBC.STG_SNC_SIEM/'||:v_start_date||'/DATABASE_REPLICATION_USAGE_HISTORY_'||:v_start_date_hour||' 
                       FROM ( SELECT OBJECT_CONSTRUCT(*) AS data 
					          FROM TABLE(INFORMATION_SCHEMA.DATABASE_REPLICATION_USAGE_HISTORY())
                              WHERE END_TIME >= CAST('''||:v_last_query_completed_time||''' AS TIMESTAMP)
                            )
							FILE_FORMAT=(TYPE=JSON COMPRESSION = NONE  NULL_IF=(''NULL'', ''null''))
                            OVERWRITE=TRUE MAX_FILE_SIZE = 3200000000;
                     ';	
  
    EXECUTE IMMEDIATE :SQL_statement;

    SQL_statement := '
                       COPY INTO @DBC.DBC.STG_SNC_SIEM/'||:v_start_date||'/DATA_TRANSFER_HISTORY_'||:v_start_date_hour||' 
                       FROM ( SELECT OBJECT_CONSTRUCT(*) AS data 
					          FROM TABLE(INFORMATION_SCHEMA.DATA_TRANSFER_HISTORY())
                              WHERE END_TIME >= CAST('''||:v_last_query_completed_time||''' AS TIMESTAMP)
                            )
							FILE_FORMAT=(TYPE=JSON COMPRESSION = NONE  NULL_IF=(''NULL'', ''null''))
                            OVERWRITE=TRUE MAX_FILE_SIZE = 3200000000;
                     ';	
  
    EXECUTE IMMEDIATE :SQL_statement;

    IF (:v_date_hour < 29) THEN

			SQL_statement := '
							   COPY INTO @DBC.DBC.STG_SNC_SIEM/'||:v_start_date||'/EXTERNAL_ACCESS_HISTORY_'||:v_start_date_hour||' 
							   FROM ( SELECT OBJECT_CONSTRUCT(*) AS data 
									  FROM SNOWFLAKE.ACCOUNT_USAGE.EXTERNAL_ACCESS_HISTORY
									)
									FILE_FORMAT=(TYPE=JSON COMPRESSION = NONE  NULL_IF=(''NULL'', ''null''))
									OVERWRITE=TRUE MAX_FILE_SIZE = 3200000000;
							 ';	
		  
			EXECUTE IMMEDIATE :SQL_statement;
			
			SQL_statement := '
							   COPY INTO @DBC.DBC.STG_SNC_SIEM/'||:v_start_date||'/NETWORK_POLICIES_'||:v_start_date_hour||' 
							   FROM ( SELECT OBJECT_CONSTRUCT(*) AS data 
									  FROM SNOWFLAKE.ACCOUNT_USAGE.NETWORK_POLICIES
									  WHERE CREATED >= CAST('''||:v_last_query_completed_time||''' AS TIMESTAMP)
									)
									FILE_FORMAT=(TYPE=JSON COMPRESSION = NONE  NULL_IF=(''NULL'', ''null''))
									OVERWRITE=TRUE MAX_FILE_SIZE = 3200000000;
							 ';	
		  
			EXECUTE IMMEDIATE :SQL_statement;
 
			SQL_statement := '
							   COPY INTO @DBC.DBC.STG_SNC_SIEM/'||:v_start_date||'/NETWORK_RULE_REFERENCES_'||:v_start_date_hour||' 
							   FROM ( SELECT OBJECT_CONSTRUCT(*) AS data 
									  FROM SNOWFLAKE.ACCOUNT_USAGE.NETWORK_RULE_REFERENCES
									)
									FILE_FORMAT=(TYPE=JSON COMPRESSION = NONE  NULL_IF=(''NULL'', ''null''))
									OVERWRITE=TRUE MAX_FILE_SIZE = 3200000000;
							 ';	
		  
			EXECUTE IMMEDIATE :SQL_statement;

			SQL_statement := '
							   COPY INTO @DBC.DBC.STG_SNC_SIEM/'||:v_start_date||'/NETWORK_RULE_'||:v_start_date_hour||' 
							   FROM ( SELECT OBJECT_CONSTRUCT(*) AS data 
									  FROM SNOWFLAKE.ACCOUNT_USAGE.NETWORK_RULES
									  WHERE CREATED >= CAST('''||:v_last_query_completed_time||''' AS TIMESTAMP)
									)
									FILE_FORMAT=(TYPE=JSON COMPRESSION = NONE  NULL_IF=(''NULL'', ''null''))
									OVERWRITE=TRUE MAX_FILE_SIZE = 3200000000;
							 ';	
		  
			EXECUTE IMMEDIATE :SQL_statement;
    
	END IF; 
	
	RETURN 'Data staged successfully.';
	
END;
$$;


CREATE TASK DBC.DBC.TASK_SP_SIEM_DATALOAD
WAREHOUSE = WH_ADMIN
SCHEDULE = 'USING CRON 15 * * * * UTC'
AS
    CALL DBC.DBC.SP_SIEM_DATALOAD();

ALTER TASK DBC.DBC.TASK_SP_SIEM_DATALOAD RESUME;