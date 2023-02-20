CREATE OR REPLACE PROCEDURE SP_GET_DDL(OBJECT_NAME VARCHAR(16777216))
RETURNS VARCHAR ()
LANGUAGE SQL
EXECUTE AS OWNER
AS 
DECLARE
res1 RESULTSET DEFAULT (SELECT PROCEDURE_CATALOG || '.' || PROCEDURE_SCHEMA || '.' || PROCEDURE_NAME || '(' || 
case when P1E IS NOT NULL then  SUBSTR(ARGUMENT_SIGNATURE,P1S, P1E-P1S) else '' end ||
case when P2E IS NOT NULL then ',' || SUBSTR(ARGUMENT_SIGNATURE,P2S, P2E-P2S) else '' end ||
case when P3E IS NOT NULL then ',' || SUBSTR(ARGUMENT_SIGNATURE,P3S, P3E-P3S) else '' end || 
case when P4E IS NOT NULL then ',' || SUBSTR(ARGUMENT_SIGNATURE,P4S, P4E-P4S) else '' end || 
case when P5E IS NOT NULL then ',' || SUBSTR(ARGUMENT_SIGNATURE,P5S, P5E-P5S) else '' end || 
case when P6E IS NOT NULL then ',' || SUBSTR(ARGUMENT_SIGNATURE,P6S, P6E-P6S) else '' end ||
case when P7E IS NOT NULL then ',' || SUBSTR(ARGUMENT_SIGNATURE,P7S, P7E-P7S) else '' end ||
case when P8E IS NOT NULL then ',' || SUBSTR(ARGUMENT_SIGNATURE,P8S, P8E-P8S) else '' end ||
case when P9E IS NOT NULL then ',' || SUBSTR(ARGUMENT_SIGNATURE,P9S, P9E-P9S) else '' end || 
case when P10E IS NOT NULL then ',' || SUBSTR(ARGUMENT_SIGNATURE,P10S, P10E-P10S) else '' end || 
' )' AS STMT1
from
( select PROCEDURE_CATALOG, PROCEDURE_SCHEMA, PROCEDURE_NAME, ARGUMENT_SIGNATURE
, NULLIFZERO(position('*',translate(ARGUMENT_SIGNATURE,' )','*,'),1)) as P1S, NULLIFZERO(position(',',translate(ARGUMENT_SIGNATURE,' )','*,'),1)) as P1E
, NULLIFZERO(position('*',translate(ARGUMENT_SIGNATURE,' )','*,'),P1E+2)) as P2S, NULLIFZERO(position(',',translate(ARGUMENT_SIGNATURE,' )','*,'),P1E+1)) as P2E
, NULLIFZERO(position('*',translate(ARGUMENT_SIGNATURE,' )','*,'),P2E+2)) as P3S, NULLIFZERO(position(',',translate(ARGUMENT_SIGNATURE,' )','*,'),P2E+1)) as P3E
, NULLIFZERO(position('*',translate(ARGUMENT_SIGNATURE,' )','*,'),P3E+2)) as P4S, NULLIFZERO(position(',',translate(ARGUMENT_SIGNATURE,' )','*,'),P3E+1)) as P4E
, NULLIFZERO(position('*',translate(ARGUMENT_SIGNATURE,' )','*,'),P4E+2)) as P5S, NULLIFZERO(position(',',translate(ARGUMENT_SIGNATURE,' )','*,'),P4E+1)) as P5E
, NULLIFZERO(position('*',translate(ARGUMENT_SIGNATURE,' )','*,'),P5E+2)) as P6S, NULLIFZERO(position(',',translate(ARGUMENT_SIGNATURE,' )','*,'),P5E+1)) as P6E
, NULLIFZERO(position('*',translate(ARGUMENT_SIGNATURE,' )','*,'),P6E+2)) as P7S, NULLIFZERO(position(',',translate(ARGUMENT_SIGNATURE,' )','*,'),P6E+1)) as P7E
, NULLIFZERO(position('*',translate(ARGUMENT_SIGNATURE,' )','*,'),P7E+2)) as P8S, NULLIFZERO(position(',',translate(ARGUMENT_SIGNATURE,' )','*,'),P7E+1)) as P8E
, NULLIFZERO(position('*',translate(ARGUMENT_SIGNATURE,' )','*,'),P8E+2)) as P9S, NULLIFZERO(position(',',translate(ARGUMENT_SIGNATURE,' )','*,'),P8E+1)) as P9E
, NULLIFZERO(position('*',translate(ARGUMENT_SIGNATURE,' )','*,'),P9E+2)) as P10S, NULLIFZERO(position(',',translate(ARGUMENT_SIGNATURE,' )','*,'),P9E+1)) as P10E
from INFORMATION_SCHEMA.PROCEDURES where PROCEDURE_NAME=:OBJECT_NAME
 )A);
res2 RESULTSET ;
c1 CURSOR FOR res1;
--c2 CURSOR FOR res2;
row_vr1 varchar default '';
row_vr2 varchar default '';
SQL_stmt varchar ;

BEGIN
FOR row_variable1 IN c1 DO
 row_vr1 := row_variable1.STMT1;
END FOR;

SQL_stmt:= 'SELECT get_ddl(''PROCEDURE'',''' || :row_vr1 || ''') as STMT2;';

res2:=(execute immediate :SQL_stmt);

LET c2 CURSOR FOR res2;

FOR row_variable2 IN c2 DO
 row_vr2 := row_variable2.STMT2;
END FOR;

return :row_vr2;

END;
