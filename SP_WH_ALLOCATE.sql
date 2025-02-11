CREATE OR REPLACE PROCEDURE DBNAME.SCHNAME.SP_WH_ALLOCATE(BUS_FUNCTION_GROUP VARCHAR(20), BATCH_GROUP VARCHAR(20))
RETURNS VARCHAR(100)
LANGUAGE SQL
EXECUTE AS CALLER
AS 
DECLARE
res1 RESULTSET DEFAULT (SELECT WH_ALLOCATE as STMT from DBNAME.SCHNAME.EDL_PROC_CONTROL where BUS_FUNCTION_GROUP=:BUS_FUNCTION_GROUP and BATCH_GROUP=:BATCH_GROUP);
res2 RESULTSET DEFAULT (SELECT CURRENT_WAREHOUSE() as CURRWH);
c1 CURSOR FOR res1;
c2 CURSOR FOR res2;
row_vr varchar default 'WH_COMPUTE';
SQL_stmt varchar;

BEGIN 
  FOR row_variable IN c1 DO
    row_vr := row_variable.STMT;
  END FOR;

  if (:row_vr = 'XSMALL' or :row_vr = 'SMALL' or :row_vr = 'MEDIUM' or :row_vr = 'LARGE') then
      SQL_Stmt:= 'call SCHNAME.SP_WH_RESIZE(''' || :row_vr || ''');';     
      execute immediate :SQL_Stmt;
      return 'Executing in updated WH: ' || :row_vr;
  end if;
  FOR row_var IN c2 DO
    if ( row_var.CURRWH = :row_vr ) then
        return 'Executing in same WH: ' || :row_vr;
    else
        SQL_Stmt:= 'USE WAREHOUSE ' || :row_vr;
        execute immediate :SQL_Stmt;
    end if;
  END FOR;
    return 'Executing in updated WH: ' || :row_vr;
EXCEPTION
WHEN statement_error THEN
    LET PROC_OP := SQLCODE || ': ' || SQLERRM;
    return :PROC_OP;
WHEN OTHER THEN
    LET PROC_OP := SQLCODE || ': ' || SQLERRM;
    return :PROC_OP;
END;
