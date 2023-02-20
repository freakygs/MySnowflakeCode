

/* CREATION OF A PROCEDURE CONTROL LOG TABLE */
create TABLE PROC_CONTROL_LOG (
	BUS_FUNCTION_GROUP VARCHAR(20) NOT NULL COLLATE 'en-ci',
	BATCH_GROUP VARCHAR(20) NOT NULL COLLATE 'en-ci',
	SNC_NAMESPACE VARCHAR(100) NOT NULL COLLATE 'en-ci',
	SNC_TABLENAME VARCHAR(50) NOT NULL COLLATE 'en-ci',
	PARAM_START_DT DATE NOT NULL,
	PARAM_END_DT DATE NOT NULL,
	EXECUTION_START_TS TIMESTAMP_NTZ(0) NOT NULL DEFAULT CURRENT_TIMESTAMP(0),
	EXECUTION_END_TS TIMESTAMP_NTZ(0),
	EXECUTION_STATUS_CD VARCHAR(1) NOT NULL COLLATE 'en-ci' DEFAULT 'R',
	PROC_OUTPUT VARCHAR(4000) COLLATE 'en-ci-trim'
);

/* CREATION OF A PROCEDURE EXECUTION LOG TABLE */
create TABLE PROC_EXEC_LOG (
	BUS_FUNCTION_GROUP VARCHAR(20) NOT NULL COLLATE 'en-ci',
	BATCH_GROUP VARCHAR(20) NOT NULL COLLATE 'en-ci',
	SP_NAME VARCHAR(100) NOT NULL COLLATE 'en-ci',
	EXECUTION_TS TIMESTAMP_NTZ(0),
  	STATEMENT_LOG VARCHAR(100) NOT NULL COLLATE 'en-ci' default '',
	PROC_OUTPUT VARCHAR(4000) COLLATE 'en-ci-trim'
);



/* PROCEDURE FOR CAPTURING PROCESS INSERT UPDATE TO TRACK CONTROL */
CREATE or replace PROCEDURE SP_PROC_CNTL_LOG
		(OPCODE NUMBER(38,0), 
		 FUNCTION_GROUP VARCHAR(20), 
		 BATCH_GROUP VARCHAR(20), 
		 SP_NAME VARCHAR(50), 
		 SNC_NAMESPACE VARCHAR(100), 
		 SNC_TABLENAME VARCHAR(50), 
		 PARAM_START_DT DATE, 
		 PARAM_END_DT DATE, 
		 STMT_LOG VARCHAR(500), 
		 PROC_OUTPUT VARCHAR(16777216)
		)
RETURNS VARCHAR(16777216)
LANGUAGE SQL
EXECUTE AS CALLER
AS 

BEGIN
  CASE
    WHEN :OPCODE=0 THEN

        INSERT INTO PROC_CONTROL_LOG
            (BUS_FUNCTION_GROUP,BATCH_GROUP,SP_NAME,SNC_NAMESPACE,SNC_TABLENAME,PARAM_START_DT,PARAM_END_DT, EXECUTION_STATUS_CD,  STATEMENT_LOG)
        VALUES (:FUNCTION_GROUP, :BATCH_GROUP, :SP_NAME, :SNC_NAMESPACE, :SNC_TABLENAME, :PARAM_START_DT, :PARAM_END_DT, 'R', :STMT_LOG);
        call SP_WH_ALLOCATE(:FUNCTION_GROUP,:BATCH_GROUP);
    
    WHEN :OPCODE=1 THEN
	call SP_WH_ALLOCATE('DEFAULT','DEFAULT'); 
        UPDATE PROC_CONTROL_LOG SET EXECUTION_END_TS=CURRENT_TIMESTAMP(0),
            EXECUTION_STATUS_CD='C',
            PROC_OUTPUT=:PROC_OUTPUT WHERE 
            BUS_FUNCTION_GROUP=:FUNCTION_GROUP AND
            BATCH_GROUP=:BATCH_GROUP AND
            SNC_NAMESPACE=:SNC_NAMESPACE AND
            SNC_TABLENAME=:SNC_TABLENAME AND
            EXECUTION_START_TS=(SELECT MAX(EXECUTION_START_TS) FROM PROC_CONTROL_LOG  WHERE 
								SNC_NAMESPACE=:SNC_NAMESPACE AND SNC_TABLENAME=:SNC_TABLENAME);
            
            INSERT INTO PROC_EXEC_LOG
                (BUS_FUNCTION_GROUP, BATCH_GROUP, SP_NAME, EXECUTION_TS, STATEMENT_LOG, PROC_OUTPUT)
            VALUES (:FUNCTION_GROUP, :BATCH_GROUP, :SP_NAME, CURRENT_TIMESTAMP(), :PROC_OUTPUT, :PROC_OUTPUT);
    
	WHEN :OPCODE=5 THEN

        INSERT INTO PROC_EXEC_LOG
            (BUS_FUNCTION_GROUP, BATCH_GROUP, SP_NAME, EXECUTION_TS, STATEMENT_LOG, PROC_OUTPUT)
        VALUES (:FUNCTION_GROUP, :BATCH_GROUP, :SP_NAME, CURRENT_TIMESTAMP(), :STMT_LOG, :PROC_OUTPUT);
  
    ELSE
	call WH_ALLOCATE('DEFAULT','DEFAULT'); 
        UPDATE PROC_CONTROL_LOG SET EXECUTION_STATUS_CD='F',
            PROC_OUTPUT=:PROC_OUTPUT WHERE 
            BUS_FUNCTION_GROUP=:FUNCTION_GROUP AND
            BATCH_GROUP=:BATCH_GROUP AND
            SNC_NAMESPACE=:SNC_NAMESPACE AND
            SNC_TABLENAME=:SNC_TABLENAME AND
            EXECUTION_START_TS=(SELECT MAX(EXECUTION_START_TS) FROM PROC_CONTROL_LOG  WHERE 
								SNC_NAMESPACE=:SNC_NAMESPACE AND SNC_TABLENAME=:SNC_TABLENAME);
  END;
END;




/* FUNCTIONAL PROCEDURE FOR INSERT/UPDATE/MERGE WITH LOGGING */

CREATE PROCEDURE SP_SOME_PROCEDURE( IN_START_DATE DATE, IN_END_DATE DATE, IN_TZ VARCHAR)
RETURNS varchar
language SQL 
AS
DECLARE
BUS_FUN varchar default 'BUS FUN';
GRP varchar default 'GRP';
NMSP varchar default 'TABLE_NAMESPACE';
TNAME varchar default 'TABLE_NAME';
SPNAME varchar default 'SP_SOME_PROCEDURE';
QID varchar default '';
BEGIN

call SP_PROC_CNTL_LOG (0,:BUS_FUN,:GRP,:NMSP,:TNAME,:IN_START_DATE,:IN_END_DATE,:SPNAME,'');

DELETE FROM
    TEMP_TABLE TMP 
						 USING TARGET_TABLE TGT
						 WHERE
								TMP.COL1 = TGT.COL1
								AND TMP.COL2 = TGT.COL2;				
                
QID := LAST_QUERY_ID();   
call SP_PROC_CNTL_LOG (5,:BUS_FUN,:GRP,'Delete Complete',:TNAME,:IN_START_DATE,:IN_END_DATE,:SPNAME,:QID);  

UPDATE TARGET_TABLE TGT
					  SET TGT.COLUMN4 = TMP.COLUMN4 ,
					  TGT.COLUMN5 = TMP.COLUMN5 ,
						FROM (
							SELECT	COLUMN4, COLUMN5 
							FROM TEMP_TABLE
							) TMP
						WHERE TGT.COL1 = TMP.COL1
						AND TGT.COL2 = TMP.COL2;
                        
QID := LAST_QUERY_ID();   
call SP_PROC_CNTL_LOG (5,:BUS_FUN,:GRP,'Update Complete',:TNAME,:IN_START_DATE,:IN_END_DATE,:SPNAME,:QID);    

INSERT INTO TARGET_TABLE
					(  COL1, COL2, COL3, COLUMN4, COLUMN5 )
					SELECT 
           COL1, COL2, COL3, COLUMN4, COLUMN5
					FROM INPUT_TABLE;
                    
QID := LAST_QUERY_ID();
call SP_PROC_CNTL_LOG (1,:BUS_FUN,:GRP,:NMSP,:TNAME,:IN_START_DATE,:IN_END_DATE,:SPNAME,:QID);


RETURN :QID;

EXCEPTION
WHEN statement_error THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
    call SP_PROC_CNTL_LOG (-1,:BUS_FUN,:GRP,:NMSP,:TNAME,:IN_START_DATE,:IN_END_DATE,:SPNAME,:LINE);
    RAISE;
WHEN OTHER THEN
    LET LINE := SQLCODE || ': ' || SQLERRM;
    call SP_PROC_CNTL_LOG (-1,:BUS_FUN,:GRP,:NMSP,:TNAME,:IN_START_DATE,:IN_END_DATE,:SPNAME,:LINE);
    RAISE;

END;
