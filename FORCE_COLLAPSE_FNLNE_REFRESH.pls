create or replace PROCEDURE FORCE_COLLAPSE_FNLNE_REFRESH
/*------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--This procedure truncates all staging tables used for the FNLNE----------------
--Force Collapse process, and then calls each insert procedure in the----------- 
--FORCE_COLLAPSE_FNLNE package. If a DML error is encountered during the--------
--execution of an insert, the DML error will be logged in the table-------------
--F_COLLAPSE_FNLNE_ERRLOG, execution will terminate, and all staging tables will 
--be truncated------------------------------------------------------------------
-------------------------------------------------------------------------------
------------------------------------------------------------------------------*/
IS
  V_ERR_COUNT NUMBER; /*+ Declare variable that will be used as a counter for DML errors */
BEGIN
  /*+ Call FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_TRUNC to truncate all staging tables to prepare for refresh */
  FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_TRUNC;
  /*+ Execute first procedure */
  FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_SUSP;
  SELECT COUNT ( 1 ) INTO V_ERR_COUNT FROM F_COLLAPSE_FNLNE_ERRLOG; /*+ Check count of logged DML errors */
  IF V_ERR_COUNT > 0 THEN /*+ If the error count is greater than zero, call FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_TRUNC to truncate all tables and exit procedure */
    FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_TRUNC;
    COMMIT; /*+ commit all logged DML errors to F_COLLAPSE_FNLNE_ERRLOG */
    RETURN; /*+ Exit refresh procedure if any DML errors occur */
  END IF;
  /*+ Execute second procedure */
  FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_SUSP_R;
  SELECT COUNT ( 1 ) INTO V_ERR_COUNT FROM F_COLLAPSE_FNLNE_ERRLOG; 
  IF V_ERR_COUNT > 0 THEN 
    FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_TRUNC;
    COMMIT; 
    RETURN;
  END IF;
  /*+ Execute third procedure */
  FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_NONSUSP;
  SELECT COUNT ( 1 ) INTO V_ERR_COUNT FROM F_COLLAPSE_FNLNE_ERRLOG;
  IF V_ERR_COUNT > 0 THEN
    FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_TRUNC;
    COMMIT;
    RETURN;
  END IF;
  /*+ Execute fourth procedure */
  FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_MATCH;
  SELECT COUNT ( 1 ) INTO V_ERR_COUNT FROM F_COLLAPSE_FNLNE_ERRLOG;
  IF V_ERR_COUNT > 0 THEN
    FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_TRUNC;
    COMMIT;
    RETURN;
  END IF;
  /*+ Execute fifth procedure */
  FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_ADDR;
  SELECT COUNT ( 1 ) INTO V_ERR_COUNT FROM F_COLLAPSE_FNLNE_ERRLOG;
  IF V_ERR_COUNT > 0 THEN
    FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_TRUNC;
    COMMIT;
    RETURN;
  END IF;
  /*+ Execute sixth procedure */
  FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_CONTMETH;
  SELECT COUNT ( 1 ) INTO V_ERR_COUNT FROM F_COLLAPSE_FNLNE_ERRLOG;
  IF V_ERR_COUNT > 0 THEN
    FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_TRUNC;
    COMMIT;
    RETURN;
  END IF;
  /*+ Execute seventh procedure */
  FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_ACCT;
  SELECT COUNT ( 1 ) INTO V_ERR_COUNT FROM F_COLLAPSE_FNLNE_ERRLOG;
  IF V_ERR_COUNT > 0 THEN
    FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_TRUNC;
    COMMIT;
    RETURN;
  END IF;
  /*+ Execute eigth procedure */
  FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_ELIG;
  SELECT COUNT ( 1 ) INTO V_ERR_COUNT FROM F_COLLAPSE_FNLNE_ERRLOG;
  IF V_ERR_COUNT > 0 THEN
    FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_TRUNC;
    COMMIT;
    RETURN;
  END IF;
  /*+ Execute ninth procedure */
  FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_MINATTRIB;
  SELECT COUNT ( 1 ) INTO V_ERR_COUNT FROM F_COLLAPSE_FNLNE_ERRLOG;
  IF V_ERR_COUNT > 0 THEN
    FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_TRUNC;
    COMMIT;
    RETURN;
  END IF;
  /*+ Execute tenth procedure */
  FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_STAGE;
  SELECT COUNT ( 1 ) INTO V_ERR_COUNT FROM F_COLLAPSE_FNLNE_ERRLOG;
  IF V_ERR_COUNT > 0 THEN
    FORCE_COLLAPSE_FNLNE.FORCE_COLLAPSE_FNLNE_TRUNC;
    COMMIT; 
    RETURN;
  END IF;
END FORCE_COLLAPSE_FNLNE_REFRESH;