create or replace PACKAGE BODY FORCE_COLLAPSE_FNLNE
AS
/*------------------------------------------------------------------------------
--------------------------------------------------------------------------------
----This package will truncate and insert to several staging tables used for----
----the First Name plus Last Name Plust Email Force Collase process-------------
--------------------------------------------------------------------------------
------------------------------------------------------------------------------*/

/*------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--This procedure truncates all FNLNE Force Collapse staging tables to prepare---
--for refresh
--------------------------------------------------------------------------------
------------------------------------------------------------------------------*/
  PROCEDURE FORCE_COLLAPSE_FNLNE_TRUNC
  IS
  BEGIN
    /*+ Truncate all FNLNE Force Collapse staging tables to prepare for refresh */
    EXECUTE IMMEDIATE 'TRUNCATE TABLE F_COLLAPSE_FNLNE_SUSP';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE F_COLLAPSE_FNLNE_SUSP_R';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE F_COLLAPSE_FNLNE_NONSUSP';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE F_COLLAPSE_FNLNE_MATCH';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE F_COLLAPSE_FNLNE_ADDR';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE F_COLLAPSE_FNLNE_CONTMETH';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE F_COLLAPSE_FNLNE_ACCT';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE F_COLLAPSE_FNLNE_ELIG';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE F_COLLAPSE_FNLNE_MINATTRIB';
    EXECUTE IMMEDIATE 'TRUNCATE TABLE F_COLLAPSE_FNLNE_STAGE';
  END FORCE_COLLAPSE_FNLNE_TRUNC;
/*------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--Insert into F_COLLAPSE_FNLNE_SUSP. This table contains suspect CONT_IDs that--
--only have a First Name, Last Name, and Email----------------------------------
--------------------------------------------------------------------------------
------------------------------------------------------------------------------*/
  PROCEDURE FORCE_COLLAPSE_FNLNE_SUSP
  IS
  /*+ Declare variables to be used for error logging at end of procedure */
    V_TABLE_NAME CHAR ( 26 ) := 'F_COLLAPSE_FNLNE_SUSP' ; /*+ Create string constant for the table being inserted into */
    V_ERR_CODE   VARCHAR2 ( 10 ) ; /*+ Create error code variable for any ORA codes that arise for a DML error */
    V_ERR_MSG    VARCHAR2 ( 255 ) ; /*+ Create error message variable for any ORA codes that aries for a DML error */
    V_CREATED_ON TIMESTAMP ( 6 ); /*+ Create a timestamp variable to capture the timestamp of when the DML error occcured */
  BEGIN
    INSERT /*+ PARALLEL(AUTO) APPEND */
    INTO F_COLLAPSE_FNLNE_SUSP
      ( CONT_ID
      )
    WITH ACTIVE_NAMES AS
      (
        SELECT C.CONT_ID
        FROM MDMMGR.CONTACT C
        INNER JOIN MDMMGR.PERSONNAME PN
        ON C.CONT_ID              = PN.CONT_ID
        WHERE PN.NAME_USAGE_TP_CD = 1000001 /*+ Primary name type */
        AND PN.END_DT            IS NULL /*+ Name is active */
        AND C.INACTIVATED_DT     IS NULL /*+ Active guests */
      )
      ,
      ACTIVE_LOC_GRPS AS
      (
        SELECT LG.LOCATION_GROUP_ID,
          AN.CONT_ID,
          LG.LOC_GROUP_TP_CODE
        FROM ACTIVE_NAMES AN
        INNER JOIN MDMMGR.LOCATIONGROUP LG
        ON AN.CONT_ID    = LG.CONT_ID
        WHERE LG.END_DT IS NULL /*+ Active Location Groups */
      )
      ,
      ACTIVE_CONTACT_METHODS AS
      (
        SELECT AGLG.CONT_ID,
          CM.CONT_METH_CAT_CD
        FROM ACTIVE_LOC_GRPS AGLG
        INNER JOIN MDMMGR.CONTACTMETHODGROUP CMG
        ON AGLG.LOCATION_GROUP_ID = CMG.LOCATION_GROUP_ID
        INNER JOIN MDMMGR.CONTACTMETHOD CM
        ON CMG.CONTACT_METHOD_ID = CM.CONTACT_METHOD_ID
      )
    SELECT
      /*+ PARALLEL(AUTO) MATERIALIZE */
      AN.CONT_ID
    FROM ACTIVE_NAMES AN
    WHERE NOT EXISTS /*+ Cannot have a mailing address */
      (
        SELECT 1
        FROM ACTIVE_LOC_GRPS AGLG
        WHERE AN.CONT_ID           = AGLG.CONT_ID
        AND AGLG.LOC_GROUP_TP_CODE = 'A'
      )
    AND EXISTS
      (
        SELECT 1
        FROM ACTIVE_CONTACT_METHODS ACM
        WHERE AN.CONT_ID         = ACM.CONT_ID
        AND ACM.CONT_METH_CAT_CD = 2 /*+ Must have an email */
      )
    MINUS
    SELECT CONT_ID FROM ACTIVE_CONTACT_METHODS ACM WHERE ACM.CONT_METH_CAT_CD = 1 /*+ Telephone category code */
    MINUS /*+ Subtract all CONT_IDs that have a financial account */
    SELECT AN.CONT_ID
    FROM ACTIVE_NAMES AN
    INNER JOIN MDMMGR.XACCOUNTROLE XAR
    ON XAR.CONT_ID = AN.CONT_ID
    INNER JOIN MDMMGR.XACCOUNTCOMPONENT XAC
    ON XAR.XACCT_COMPONENT_ID = XAC.XACCT_COMPONENT_ID
    INNER JOIN MDMMGR.XACCOUNT XA
    ON XAC.ACCOUNT_ID        = XA.ACCOUNT_ID
    WHERE XA.XACCOUNT_CAT_CD = 1000001 ; /*+ Financial account category code */
    COMMIT;
    EXCEPTION /*+ Log errors in F_COLLAPSE_FNLNE_ERRLOG if execptions occur*/
    WHEN OTHERS THEN
      V_TABLE_NAME := V_TABLE_NAME; 
      V_ERR_CODE   := SQLCODE; /*+ SQLCODE is a pseudo column used to obtain any ORA errors that arise */
      V_ERR_MSG    := SUBSTR ( SQLERRM, 1, 255 ) ; /*+ Limit the error message to 255 characters for the ORA error code */
      V_CREATED_ON := SYSTIMESTAMP; /*+ Use the timestamp at the moment the error was logged */
      INSERT
        /*+ APPEND */
      INTO F_COLLAPSE_FNLNE_ERRLOG /*+ Insert all logged errors with the respective table name, error code, error message, and timestamp */
        (
          TABLE_NAME,
          ERR_CODE,
          ERR_MSG,
          CREATED_ON
        )
        VALUES
        (
          V_TABLE_NAME,
          V_ERR_CODE,
          V_ERR_MSG,
          V_CREATED_ON
        ) ;
  END FORCE_COLLAPSE_FNLNE_SUSP;
/*+-----------------------------------------------------------------------------
--------------------------------------------------------------------------------
--Insert into F_COLLAPSE_FNLNE_SUSP_R. This table contains suspect CONT_IDs
--that the root CONT_IDs from the result of prior force collapse recursions.
--------------------------------------------------------------------------------
------------------------------------------------------------------------------*/
  PROCEDURE FORCE_COLLAPSE_FNLNE_SUSP_R
  IS
    V_TABLE_NAME CHAR ( 26 ) := 'F_COLLAPSE_FNLNE_SUSP_R' ; 
    V_ERR_CODE   VARCHAR2 ( 10 ) ; 
    V_ERR_MSG    VARCHAR2 ( 255 ) ; 
    V_CREATED_ON TIMESTAMP ( 6 ); 
  BEGIN
    INSERT /*+ PARALLEL(AUTO) APPEND */
    INTO F_COLLAPSE_FNLNE_SUSP_R
      ( CONT_ID
      )
    WITH F_COLLAPSE AS
      (
        SELECT F_CONT_ID AS CONT_ID FROM MDMMGR.FORCE_COLLAPSE WHERE CREATED_BY = 'FNLNE' /*+ Filter for Name plus Email force collapses */
        UNION ALL
        SELECT F_SUSPECT_CONT_ID AS CONT_ID FROM MDMMGR.FORCE_COLLAPSE WHERE CREATED_BY = 'FNLNE' /*+ Filter for Name plus Email force collapses */
      )
    SELECT
      /*+ PARALLEL(AUTO) */
      Q.CONT_ID
    FROM
      (
        SELECT CONNECT_BY_ROOT ( T1.TARGET_CONT_ID ) AS CONT_ID /*+ Selects the root CONT_ID, or the final, created CONT_ID from previous merges at query run time */
        FROM
          (
            SELECT SOURCE_CONT_ID,
              TARGET_CONT_ID
            FROM MDMMGR.INACTIVECONTLINK IACL
            INNER JOIN MDMMGR.CONTACT C
            ON IACL.TARGET_CONT_ID = C.CONT_ID
            WHERE EXISTS /*+ Filter for previously performed force collapses */
              (
                SELECT 1 FROM F_COLLAPSE FC WHERE IACL.SOURCE_CONT_ID = FC.CONT_ID
              )
            AND C.INACTIVATED_DT IS NULL
          )
          T1
        LEFT JOIN
          (
            SELECT SOURCE_CONT_ID, TARGET_CONT_ID FROM MDMMGR.INACTIVECONTLINK
          )
          T2
        ON T2.SOURCE_CONT_ID            = T1.TARGET_CONT_ID
          START WITH T2.TARGET_CONT_ID IS NULL /*+ Start at the top of the hierarchy */
          CONNECT BY T1.TARGET_CONT_ID  = PRIOR T1.SOURCE_CONT_ID
      )
      Q
    GROUP BY Q.CONT_ID;
    COMMIT;
    EXCEPTION 
    WHEN OTHERS THEN
      V_TABLE_NAME := V_TABLE_NAME; 
      V_ERR_CODE   := SQLCODE; 
      V_ERR_MSG    := SUBSTR ( SQLERRM, 1, 255 ) ; 
      V_CREATED_ON := SYSTIMESTAMP; 
      INSERT
        /*+ APPEND */
      INTO F_COLLAPSE_FNLNE_ERRLOG 
        (
          TABLE_NAME,
          ERR_CODE,
          ERR_MSG,
          CREATED_ON
        )
        VALUES
        (
          V_TABLE_NAME,
          V_ERR_CODE,
          V_ERR_MSG,
          V_CREATED_ON
        ) ;
  END FORCE_COLLAPSE_FNLNE_SUSP_R;
/*+-----------------------------------------------------------------------------
--------------------------------------------------------------------------------
--Insert into F_COLLAPSE_FNLNE_NONSUSP. This table contains non-suspect CONT_IDs
--with active names and emails
--------------------------------------------------------------------------------
------------------------------------------------------------------------------*/
  PROCEDURE FORCE_COLLAPSE_FNLNE_NONSUSP
  IS
    V_TABLE_NAME CHAR ( 26 ) := 'F_COLLAPSE_FNLNE_NONSUSP' ;
    V_ERR_CODE   VARCHAR2 ( 10 ) ;
    V_ERR_MSG    VARCHAR2 ( 255 ) ;
    V_CREATED_ON TIMESTAMP ( 6 ) ;
  BEGIN
    INSERT /*+ PARALLEL(AUTO) APPEND */
    INTO F_COLLAPSE_FNLNE_NONSUSP
      ( CONT_ID
      )
    WITH ACTIVE_NAMES AS /*+ Create active CONT_IDs with active names in a temp table */
      (
        SELECT C.CONT_ID
        FROM MDMMGR.CONTACT C
        INNER JOIN MDMMGR.PERSONNAME PN
        ON C.CONT_ID = PN.CONT_ID
        WHERE NOT EXISTS
          (
            SELECT 1 FROM F_COLLAPSE_FNLNE_SUSP FSS WHERE PN.CONT_ID = FSS.CONT_ID /*+ Non-suspects cannot be suspects */
          )
        AND NOT EXISTS
          (
            SELECT 1 FROM F_COLLAPSE_FNLNE_SUSP_R FCSR WHERE PN.CONT_ID = FCSR.CONT_ID /*+ Non-suspects cannot be suspects resulting from a merge */
          )
        AND PN.NAME_USAGE_TP_CD = 1000001 /*+ Primary name type */
        AND PN.END_DT          IS NULL /*+ Name is active */
        AND C.INACTIVATED_DT   IS NULL /*+ Guest is active */
      )
      ,
      ACTIVE_LOC_GRPS AS /*+ Create active CONT_IDs with active names and location groups in a temp table */
      (
        SELECT LG.LOCATION_GROUP_ID,
          AN.CONT_ID,
          LG.LOC_GROUP_TP_CODE
        FROM ACTIVE_NAMES AN
        INNER JOIN MDMMGR.LOCATIONGROUP LG
        ON AN.CONT_ID    = LG.CONT_ID
        WHERE LG.END_DT IS NULL /*+ Active Location Groups */
      )
      ,
      ACTIVE_CONTACT_METHODS AS /*+ Create active CONT_IDs with active contact methods in temp table */
      (
        SELECT AGLG.CONT_ID,
          CM.CONT_METH_CAT_CD
        FROM ACTIVE_LOC_GRPS AGLG
        INNER JOIN MDMMGR.CONTACTMETHODGROUP CMG
        ON AGLG.LOCATION_GROUP_ID = CMG.LOCATION_GROUP_ID
        INNER JOIN MDMMGR.CONTACTMETHOD CM
        ON CMG.CONTACT_METHOD_ID = CM.CONTACT_METHOD_ID
      )
    SELECT
      /*+ PARALLEL(AUTO) MATERIALIZE */
      AN.CONT_ID
    FROM ACTIVE_NAMES AN
    WHERE EXISTS /*+ Non Suspects must have an email address */
      (
        SELECT 1
        FROM ACTIVE_CONTACT_METHODS ACM
        WHERE AN.CONT_ID         = ACM.CONT_ID
        AND ACM.CONT_METH_CAT_CD = 2
      ) ;
    COMMIT;
    EXCEPTION /*+ Log errors in F_COLLAPSE_FNLNE_ERRLOG if execptions occur*/
    WHEN OTHERS THEN
      V_TABLE_NAME := V_TABLE_NAME;
      V_ERR_CODE   := SQLCODE;
      V_ERR_MSG    := SUBSTR ( SQLERRM, 1, 255 ) ;
      V_CREATED_ON := SYSTIMESTAMP;
      INSERT
        /*+ APPEND */
      INTO F_COLLAPSE_FNLNE_ERRLOG
        (
          TABLE_NAME,
          ERR_CODE,
          ERR_MSG,
          CREATED_ON
        )
        VALUES
        (
          V_TABLE_NAME,
          V_ERR_CODE,
          V_ERR_MSG,
          V_CREATED_ON
        ) ;
  END FORCE_COLLAPSE_FNLNE_NONSUSP;
/*+-----------------------------------------------------------------------------
--------------------------------------------------------------------------------
--Insert into F_COLLAPSE_FNLNE_MATCH. This table contains matched groups of
--a suspect and multiple non-suspects, where a match occurs on first name, last
--name, and email
--------------------------------------------------------------------------------
------------------------------------------------------------------------------*/
  PROCEDURE FORCE_COLLAPSE_FNLNE_MATCH
  IS
    V_TABLE_NAME CHAR ( 26 ) := 'F_COLLAPSE_FNLNE_MATCH' ;
    V_ERR_CODE   VARCHAR2 ( 10 ) ;
    V_ERR_MSG    VARCHAR2 ( 255 ) ;
    V_CREATED_ON TIMESTAMP ( 6 );
  BEGIN
    INSERT
      /*+ PARALLEL(AUTO) APPEND */
    INTO F_COLLAPSE_FNLNE_MATCH
      (
        SUSPECT_GROUP_ID,
        CONT_ID,
        SUSPECT_IND,
        CONTACT_METHOD_ID
      )
    SELECT
      /*+ PARALLEL(AUTO) */
      Q.SUSPECT_GROUP_ID,
      Q.CONT_ID,
      CASE /*+ If there is more than 1 SUSPECT_IND of 'Y' then assign a 'Y' to the MIN CONT_ID of the suspect group */
        WHEN Q.SUSPECT_IND                                                   = 'Y'
        AND COUNT ( Q.SUSPECT_IND ) OVER ( PARTITION BY Q.SUSPECT_GROUP_ID ) > 1
        THEN
          CASE
            WHEN Q.CONT_ID = MIN ( Q.CONT_ID ) OVER ( PARTITION BY Q.SUSPECT_GROUP_ID, Q.SUSPECT_IND )
            THEN 'Y'
            WHEN Q.CONT_ID > MIN ( Q.CONT_ID ) OVER ( PARTITION BY Q.SUSPECT_GROUP_ID, Q.SUSPECT_IND )
            THEN 'N'
          END
        ELSE Q.SUSPECT_IND
      END AS SUSPECT_IND,
      Q.CONTACT_METHOD_ID
    FROM
      (
        SELECT Q.SUSPECT_GROUP_ID,
          Q.CONT_ID,
          CASE /*+ If the sum of the MERGE_RECURS_COUNTER is greater than 0 for the suspect group, make the created CONT_ID the suspect, and all other CONT_IDs non-suspects */
            WHEN SUM ( Q.MERGE_RECURS_COUNTER ) OVER ( PARTITION BY Q.SUSPECT_GROUP_ID ) > 0
            AND Q.MERGE_RECURS_COUNTER                                                   = 0
            THEN 'N'
            ELSE Q.SUSPECT_IND
          END AS SUSPECT_IND,
          Q.CONTACT_METHOD_ID
        FROM
          (
            SELECT DENSE_RANK ( ) OVER ( ORDER BY Q.FIRST_NAME, Q.LAST_NAME, Q.CONTACT_METHOD_ID ) AS SUSPECT_GROUP_ID, /*+ Generates a grouping ID for the suspect and non-suspect pair(s) */
              Q.CONT_ID,
              Q.SUSPECT_IND,
              Q.MERGE_RECURS_COUNTER,
              Q.CONTACT_METHOD_ID
            FROM /*+ The SUM functions will be used as a predicate in the next outer inline view to determine matches based on the SUM partition */
              (
                SELECT Q.CONT_ID,
                  Q.SUSPECT_IND,
                  Q.MERGE_RECURS_COUNTER,
                  Q.FIRST_NAME,
                  Q.LAST_NAME,
                  Q.CONTACT_METHOD_ID,
                  SUM ( Q.SUSPECT_COUNTER ) OVER ( PARTITION BY Q.FIRST_NAME, Q.LAST_NAME, Q.CONTACT_METHOD_ID )     AS SUSPECT_SUM, /*+ Calculates the aggregate sum up to the current row*/
                  SUM ( Q.NON_SUSPECT_COUNTER ) OVER ( PARTITION BY Q.FIRST_NAME, Q.LAST_NAME, Q.CONTACT_METHOD_ID ) AS NON_SUSPECT_SUM /*+ Calculates the aggregate sum up to the current row */
                FROM
                  (
                    SELECT Q.CONT_ID,
                      Q.SUSPECT_IND,
                      Q.MERGE_RECURS_COUNTER,
                      Q.FIRST_NAME,
                      Q.LAST_NAME,
                      Q.CONTACT_METHOD_ID,
                      CASE /*+ Creates an boolean value that will be used in the next outer inline view for summation */
                        WHEN Q.SUSPECT_IND = 'Y'
                        THEN 1
                        ELSE 0
                      END AS SUSPECT_COUNTER,
                      CASE /*+ Creates an boolean value that will be used in the next outer inline view for summation */
                        WHEN Q.SUSPECT_IND = 'N'
                        THEN 1
                        ELSE 0
                      END AS NON_SUSPECT_COUNTER
                    FROM /*+ Select unique suspects and non-suspects that have a first name, last name, and email */
                      (
                        SELECT GSTS.CONT_ID,
                          GSTS.SUSPECT_IND,
                          GSTS.MERGE_RECURS_COUNTER,
                          NAMES.FIRST_NAME,
                          NAMES.LAST_NAME,
                          EMAILS.CONTACT_METHOD_ID
                        FROM /*+ Select merge candidates from the source system look up table and remove duplicates */
                          (
                            SELECT CONT_ID,
                              'Y' AS SUSPECT_IND,
                               1  AS MERGE_RECURS_COUNTER /*+ Created guests from a merge recursion will be indicated to reassign the SUSPECT_IND */
                            FROM F_COLLAPSE_FNLNE_SUSP_R
                            UNION ALL /*+ Combine suspects from a merge recursion and non-suspects */
                            SELECT CONT_ID,
                              'Y' AS SUSPECT_IND,
                               0  AS MERGE_RECURS_COUNTER
                            FROM F_COLLAPSE_FNLNE_SUSP
                            UNION ALL /*+ Combine suspects NOT from a merge recursion and non-suspects */
                            SELECT CONT_ID,
                              'N' AS SUSPECT_IND,
                               0  AS MERGE_RECURS_COUNTER
                            FROM F_COLLAPSE_FNLNE_NONSUSP
                          )
                          GSTS
                        INNER JOIN
                          (
                            SELECT CONT_ID,
                              UPPER ( TRIM ( GIVEN_NAME_ONE ) ) AS FIRST_NAME,
                              UPPER ( TRIM ( LAST_NAME ) )      AS LAST_NAME
                            FROM MDMMGR.PERSONNAME
                            WHERE NAME_USAGE_TP_CD = 1000001 /*+ Primary name type */
                            AND END_DT            IS NULL /*+ Active names */
                          )
                          NAMES
                        ON GSTS.CONT_ID = NAMES.CONT_ID
                        INNER JOIN
                          (
                            SELECT LG.CONT_ID,
                              CM.CONTACT_METHOD_ID
                            FROM MDMMGR.LOCATIONGROUP LG
                            INNER JOIN MDMMGR.CONTACTMETHODGROUP CMG
                            ON LG.LOCATION_GROUP_ID = CMG.LOCATION_GROUP_ID
                            INNER JOIN MDMMGR.CONTACTMETHOD CM
                            ON CMG.CONTACT_METHOD_ID = CM.CONTACT_METHOD_ID
                            WHERE LG.END_DT         IS NULL
                            AND CM.CONT_METH_CAT_CD  = 2 /*+ Contact Method of type Email */
                          )
                          EMAILS ON GSTS.CONT_ID = EMAILS.CONT_ID
                        GROUP BY GSTS.CONT_ID,
                          GSTS.SUSPECT_IND,
                          GSTS.MERGE_RECURS_COUNTER,
                          NAMES.FIRST_NAME,
                          NAMES.LAST_NAME,
                          EMAILS.CONTACT_METHOD_ID
                      )
                      Q
                  )
                  Q
              )
              Q
            WHERE Q.SUSPECT_SUM   > 0 /*+ If both the SUSPECT_SUM and NON_SUSPECT_SUM are greater than zero, there is a match */
            AND Q.NON_SUSPECT_SUM > 0 /*+ If both the SUSPECT_SUM and NON_SUSPECT_SUM are greater than zero, there is a match */
          )
          Q
      )
      Q;
      COMMIT;
      EXCEPTION /*+ Log errors in F_COLLAPSE_FNLNE_ERRLOG if execptions occur*/
      WHEN OTHERS THEN
        V_TABLE_NAME := V_TABLE_NAME;
        V_ERR_CODE   := SQLCODE;
        V_ERR_MSG    := SUBSTR ( SQLERRM, 1, 255 ) ;
        V_CREATED_ON := SYSTIMESTAMP;
        INSERT
          /*+ APPEND */
        INTO F_COLLAPSE_FNLNE_ERRLOG
          (
            TABLE_NAME,
            ERR_CODE,
            ERR_MSG,
            CREATED_ON
          )
          VALUES
          (
            V_TABLE_NAME,
            V_ERR_CODE,
            V_ERR_MSG,
            V_CREATED_ON
          ) ;
  END FORCE_COLLAPSE_FNLNE_MATCH;
/*+-----------------------------------------------------------------------------
--------------------------------------------------------------------------------
--Create a of staging table of non-suspect addresses for the
--F_COLLAPSE_FNLNE_MINATTRIB procedure. This will increase the performance of
--the insert into F_COLLAPSE_FNLNE_MINATTRIB
--------------------------------------------------------------------------------
------------------------------------------------------------------------------*/
  PROCEDURE FORCE_COLLAPSE_FNLNE_ADDR
  IS
    V_TABLE_NAME CHAR ( 26 ) := 'F_COLLAPSE_FNLNE_ADDR' ;
    V_ERR_CODE   VARCHAR2 ( 10 ) ;
    V_ERR_MSG    VARCHAR2 ( 255 ) ;
    V_CREATED_ON TIMESTAMP ( 6 ) ;
  BEGIN
    INSERT /*+ PARALLEL(AUTO) APPEND */
    INTO F_COLLAPSE_FNLNE_ADDR
      ( CONT_ID, ADDRESS_ID, XADDRESS_TP_CD
      )
    SELECT
      /*+ PARALLEL(AUTO) */
      FMP.CONT_ID,
      A.ADDRESS_ID,
      AG.XADDRESS_TP_CD
    FROM F_COLLAPSE_FNLNE_MATCH FMP
    INNER JOIN MDMMGR.LOCATIONGROUP LG
    ON FMP.CONT_ID = LG.CONT_ID
    INNER JOIN MDMMGR.ADDRESSGROUP AG
    ON LG.LOCATION_GROUP_ID = AG.LOCATION_GROUP_ID
    INNER JOIN MDMMGR.ADDRESS A
    ON AG.ADDRESS_ID      = A.ADDRESS_ID
    WHERE FMP.SUSPECT_IND = 'N'
    AND LG.END_DT        IS NULL /*+ Active addresses */
    GROUP BY FMP.CONT_ID,
      A.ADDRESS_ID,
      AG.XADDRESS_TP_CD;
    COMMIT;
    EXCEPTION /*+ Log errors in F_COLLAPSE_FNLNE_ERRLOG if execptions occur*/
    WHEN OTHERS THEN
      V_TABLE_NAME := V_TABLE_NAME;
      V_ERR_CODE   := SQLCODE;
      V_ERR_MSG    := SUBSTR ( SQLERRM, 1, 255 ) ;
      V_CREATED_ON := SYSTIMESTAMP;
      INSERT
        /*+ APPEND */
      INTO F_COLLAPSE_FNLNE_ERRLOG
        (
          TABLE_NAME,
          ERR_CODE,
          ERR_MSG,
          CREATED_ON
        )
        VALUES
        (
          V_TABLE_NAME,
          V_ERR_CODE,
          V_ERR_MSG,
          V_CREATED_ON
        ) ;
  END FORCE_COLLAPSE_FNLNE_ADDR;
/*+-----------------------------------------------------------------------------
--------------------------------------------------------------------------------
--Create a of staging table of non-suspect contact methods for the
--F_COLLAPSE_FNLNE_MINATTRIB procedure. This will increase the performance of 
--the insert into F_COLLAPSE_FNLNE_MINATTRIB
--------------------------------------------------------------------------------
------------------------------------------------------------------------------*/
  PROCEDURE FORCE_COLLAPSE_FNLNE_CONTMETH
  IS
    V_TABLE_NAME CHAR ( 26 ) := 'F_COLLAPSE_FNLNE_CONTMETH' ;
    V_ERR_CODE   VARCHAR2 ( 10 ) ;
    V_ERR_MSG    VARCHAR2 ( 255 ) ;
    V_CREATED_ON TIMESTAMP ( 6 ) ;
  BEGIN
    INSERT /*+ PARALLEL(AUTO) APPEND */
    INTO F_COLLAPSE_FNLNE_CONTMETH
      ( CONT_ID, CONTACT_METHOD_ID, CONT_METH_CAT_CD
      )
    SELECT
      /*+ PARALLEL(AUTO) */
      FMP.CONT_ID,
      CM.CONTACT_METHOD_ID,
      CM.CONT_METH_CAT_CD
    FROM F_COLLAPSE_FNLNE_MATCH FMP
    INNER JOIN MDMMGR.LOCATIONGROUP LG
    ON FMP.CONT_ID = LG.CONT_ID
    INNER JOIN MDMMGR.CONTACTMETHODGROUP CMG
    ON LG.LOCATION_GROUP_ID = CMG.LOCATION_GROUP_ID
    INNER JOIN MDMMGR.CONTACTMETHOD CM
    ON CMG.CONTACT_METHOD_ID = CM.CONTACT_METHOD_ID
    WHERE FMP.SUSPECT_IND    = 'N'
    AND LG.END_DT           IS NULL /*+ Active phones and emails */
    GROUP BY FMP.CONT_ID,
      CM.CONTACT_METHOD_ID,
      CM.CONT_METH_CAT_CD;
    COMMIT;
    EXCEPTION /*+ Log errors in F_COLLAPSE_FNLNE_ERRLOG if execptions occur*/
    WHEN OTHERS THEN
      V_TABLE_NAME := V_TABLE_NAME;
      V_ERR_CODE   := SQLCODE;
      V_CREATED_ON := SYSTIMESTAMP;
      INSERT
        /*+ APPEND */
      INTO F_COLLAPSE_FNLNE_ERRLOG
        (
          TABLE_NAME,
          ERR_CODE,
          ERR_MSG,
          CREATED_ON
        )
        VALUES
        (
          V_TABLE_NAME,
          V_ERR_CODE,
          V_ERR_MSG,
          V_CREATED_ON
        ) ;
  END FORCE_COLLAPSE_FNLNE_CONTMETH;
/*+-----------------------------------------------------------------------------
--------------------------------------------------------------------------------
--Create a of staging table of non-suspect accounts for the 
--F_COLLAPSE_FNLNE_MINATTRIB procedure. This will increase the performance of 
--the insert into F_COLLAPSE_FNLNE_MINATTRIB
--------------------------------------------------------------------------------
------------------------------------------------------------------------------*/
  PROCEDURE FORCE_COLLAPSE_FNLNE_ACCT
  IS
    V_TABLE_NAME CHAR ( 26 ) := 'F_COLLAPSE_FNLNE_ACCT' ;
    V_ERR_CODE   VARCHAR2 ( 10 ) ;
    V_ERR_MSG    VARCHAR2 ( 255 ) ;
    V_CREATED_ON TIMESTAMP ( 6 ) ;
  BEGIN
    INSERT /*+ PARALLEL(AUTO) APPEND */
    INTO F_COLLAPSE_FNLNE_ACCT
      ( CONT_ID, ACCOUNT_NUM, XACCOUNT_TP_CD
      )
    SELECT
      /*+ PARALLEL(AUTO) */
      FMP.CONT_ID,
      XAC.ACCOUNT_NUM,
      XAC.XACCOUNT_TP_CD
    FROM F_COLLAPSE_FNLNE_MATCH FMP
    INNER JOIN MDMMGR.XACCOUNTROLE XAR
    ON FMP.CONT_ID = XAR.CONT_ID
    INNER JOIN MDMMGR.XACCOUNTCOMPONENT XAC
    ON XAR.XACCT_COMPONENT_ID = XAC.XACCT_COMPONENT_ID
    WHERE FMP.SUSPECT_IND     = 'N'
    AND XAC.XACCOUNT_TP_CD   <> 1000022 /*+ Exclude Rx Rewards as this is decommissioned data */
    GROUP BY FMP.CONT_ID,
      XAC.ACCOUNT_NUM,
      XAC.XACCOUNT_TP_CD;
    COMMIT;
    EXCEPTION /*+ Log errors in F_COLLAPSE_FNLNE_ERRLOG if execptions occur*/
    WHEN OTHERS THEN
      V_TABLE_NAME := V_TABLE_NAME;
      V_ERR_CODE   := SQLCODE;
      V_ERR_MSG    := SUBSTR ( SQLERRM, 1, 255 ) ;
      V_CREATED_ON := SYSTIMESTAMP;
      INSERT
        /*+ APPEND */
      INTO F_COLLAPSE_FNLNE_ERRLOG
        (
          TABLE_NAME,
          ERR_CODE,
          ERR_MSG,
          CREATED_ON
        )
        VALUES
        (
          V_TABLE_NAME,
          V_ERR_CODE,
          V_ERR_MSG,
          V_CREATED_ON
        ) ;
  END FORCE_COLLAPSE_FNLNE_ACCT;
/*+-----------------------------------------------------------------------------
--------------------------------------------------------------------------------
--Create an additional staging table for the F_COLLAPSE_FNLNE_MINATTRIB 
--procedure. This also increase the performance of the insert into 
--F_COLLAPSE_FNLNE_MINATTRIB. This procedure will capture non-suspect CONT_IDs
--that have at least an address, phone, multiple emails, or an account
--------------------------------------------------------------------------------
------------------------------------------------------------------------------*/
  PROCEDURE FORCE_COLLAPSE_FNLNE_ELIG
  IS
    V_TABLE_NAME CHAR ( 26 ) := 'F_COLLAPSE_FNLNE_ELIG' ;
    V_ERR_CODE   VARCHAR2 ( 10 ) ;
    V_ERR_MSG    VARCHAR2 ( 255 ) ;
    V_CREATED_ON TIMESTAMP ( 6 ) ;
  BEGIN
    INSERT /*+ PARALLEL(AUTO) APPEND */
    INTO F_COLLAPSE_FNLNE_ELIG
      ( CONT_ID
      )
    WITH FNLNEM_CONTACTS AS
      (
        SELECT SUSPECT_GROUP_ID, CONT_ID, SUSPECT_IND, CONTACT_METHOD_ID FROM F_COLLAPSE_FNLNE_MATCH
      )
    SELECT
      /*+ PARALLEL(AUTO) MATERIALIZE */
      Q.CONT_ID
    FROM /*+ Select all non suspect CONT_IDs that at only multiple emails that don't match the suspect email */
      (
        SELECT Q.CONT_ID
        FROM
          (
            SELECT FC.SUSPECT_GROUP_ID,
              FC.CONT_ID,
              FNC.CONTACT_METHOD_ID
            FROM FNLNEM_CONTACTS FC
            INNER JOIN F_COLLAPSE_FNLNE_CONTMETH FNC
            ON FC.CONT_ID        = FNC.CONT_ID
            WHERE FC.SUSPECT_IND = 'N'
            AND NOT EXISTS /*+ This series of EXISTS statements will isolate non-suspect CONT_IDs with only multiple emails */
              (
                SELECT 1 FROM F_COLLAPSE_FNLNE_CONTMETH FSC WHERE FC.CONT_ID = FSC.CONT_ID AND CONT_METH_CAT_CD = 1 /*+ Cannot have a phone */
              )
            AND NOT EXISTS
              (
                SELECT 1 FROM F_COLLAPSE_FNLNE_ADDR FSA WHERE FC.CONT_ID = FSA.CONT_ID /*+ Cannot have an address */
              )
            AND NOT EXISTS
              (
                SELECT 1 FROM F_COLLAPSE_FNLNE_ACCT FSAC WHERE FC.CONT_ID = FSAC.CONT_ID /*+ Cannot have an account */
              )
          )
          Q
        LEFT JOIN /*+ If only multiple emails exists for the non-suspect, they cannot match the suspects email */
          (
            SELECT FC.SUSPECT_GROUP_ID, FC.CONTACT_METHOD_ID FROM FNLNEM_CONTACTS FC WHERE FC.SUSPECT_IND = 'Y'
          )
          SE
        ON Q.SUSPECT_GROUP_ID       = SE.SUSPECT_GROUP_ID
        AND Q.CONTACT_METHOD_ID     = SE.CONTACT_METHOD_ID
        WHERE SE.CONTACT_METHOD_ID IS NULL
        UNION ALL /*+ Select all non suspect CONT_IDs that at least have a phone, email, account or multiple emails */
        SELECT FC.CONT_ID
        FROM FNLNEM_CONTACTS FC
        WHERE FC.SUSPECT_IND = 'N'
        AND /*+ Must have at least a phone, address, or account */
          (
            EXISTS
            (
              SELECT 1 FROM F_COLLAPSE_FNLNE_CONTMETH FSC WHERE FC.CONT_ID = FSC.CONT_ID AND CONT_METH_CAT_CD = 1
            )
          OR EXISTS
            (
              SELECT 1 FROM F_COLLAPSE_FNLNE_ADDR FSA WHERE FC.CONT_ID = FSA.CONT_ID
            )
          OR EXISTS
            (
              SELECT 1 FROM F_COLLAPSE_FNLNE_ACCT FSAC WHERE FC.CONT_ID = FSAC.CONT_ID
            )
          )
      )
      Q
    GROUP BY Q.CONT_ID;
    COMMIT;
    EXCEPTION /*+ Log errors in F_COLLAPSE_FNLNE_ERRLOG if execptions occur*/
    WHEN OTHERS THEN
      V_TABLE_NAME := V_TABLE_NAME;
      V_ERR_CODE   := SQLCODE;
      V_ERR_MSG    := SUBSTR ( SQLERRM, 1, 255 ) ;
      V_CREATED_ON := SYSTIMESTAMP;
      INSERT
        /*+ APPEND */
      INTO F_COLLAPSE_FNLNE_ERRLOG
        (
          TABLE_NAME,
          ERR_CODE,
          ERR_MSG,
          CREATED_ON
        )
        VALUES
        (
          V_TABLE_NAME,
          V_ERR_CODE,
          V_ERR_MSG,
          V_CREATED_ON
        ) ;
  END FORCE_COLLAPSE_FNLNE_ELIG;
/*+-----------------------------------------------------------------------------
--------------------------------------------------------------------------------
--Insert into F_COLLAPSE_FNLNE_MINATTRIB. This table selects CONT_IDs from
--FORCE_COLLAPSE_FNLNE_ELIG that have a distinct combination of non-null phones,
--emails, or mailing addresses
--------------------------------------------------------------------------------
------------------------------------------------------------------------------*/
  PROCEDURE FORCE_COLLAPSE_FNLNE_MINATTRIB
  IS
    V_TABLE_NAME CHAR ( 26 ) := 'F_COLLAPSE_FNLNE_MINATTRIB' ;
    V_ERR_CODE   VARCHAR2 ( 10 ) ;
    V_ERR_MSG    VARCHAR2 ( 255 ) ;
    V_CREATED_ON TIMESTAMP ( 6 ) ;
  BEGIN
    INSERT /*+ PARALLEL(AUTO) APPEND */
    INTO F_COLLAPSE_FNLNE_MINATTRIB
      ( CONT_ID
      )
    SELECT
      /*+ PARALLEL(AUTO) */
      DISTINCT Q.CONT_ID
    FROM
      (
        SELECT FNE.CONT_ID,
          ROW_NUMBER ( ) OVER ( PARTITION BY ADDR.ADDRESS_ID, CONTMETHODS.CONTACT_METHOD_ID, ACCT.ACCOUNT_NUM, ACCT.XACCOUNT_TP_CD ORDER BY ROWNUM NULLS LAST ) AS RWNUM /*+ Order attributes to filter for unique groups of addresses, phones, and accounts */
        FROM F_COLLAPSE_FNLNE_ELIG FNE
        LEFT JOIN F_COLLAPSE_FNLNE_ADDR ADDR /*+ Left join to return all of the F_COLLAPSE_FNLNE_ELIG */
        ON FNE.CONT_ID = ADDR.CONT_ID
        LEFT JOIN F_COLLAPSE_FNLNE_CONTMETH CONTMETHODS /*+ Left join to return all of the F_COLLAPSE_FNLNE_ELIG */
        ON FNE.CONT_ID = CONTMETHODS.CONT_ID
        LEFT JOIN F_COLLAPSE_FNLNE_ACCT ACCT /*+ Left join to return all of the F_COLLAPSE_FNLNE_ELIG */
        ON FNE.CONT_ID = ACCT.CONT_ID
      )
      Q
    WHERE Q.RWNUM = 1 ; /*+ Select only one unqiue combination from groups of addresses, phones, and accounts */
    COMMIT;
    EXCEPTION /*+ Log errors in F_COLLAPSE_FNLNE_ERRLOG if execptions occur*/
    WHEN OTHERS THEN
      V_TABLE_NAME := V_TABLE_NAME;
      V_ERR_CODE   := SQLCODE;
      V_ERR_MSG    := SUBSTR ( SQLERRM, 1, 255 ) ;
      V_CREATED_ON := SYSTIMESTAMP;
      INSERT
        /*+ APPEND */
      INTO F_COLLAPSE_FNLNE_ERRLOG
        (
          TABLE_NAME,
          ERR_CODE,
          ERR_MSG,
          CREATED_ON
        )
        VALUES
        (
          V_TABLE_NAME,
          V_ERR_CODE,
          V_ERR_MSG,
          V_CREATED_ON
        ) ;
  END FORCE_COLLAPSE_FNLNE_MINATTRIB;
/*+-----------------------------------------------------------------------------
--------------------------------------------------------------------------------
--Insert into F_COLLAPSE_FNLNE_STAGE. This table transposes data from
--F_COLLAPSE_FNLNE_MATCH where the CONT_ID exists in F_COLLAPSE_FNLNE_MINATTRIB
--The data needs to be transposed for insert into F_COLLAPSE
--------------------------------------------------------------------------------
------------------------------------------------------------------------------*/
  PROCEDURE FORCE_COLLAPSE_FNLNE_STAGE
  IS
    V_TABLE_NAME CHAR ( 26 ) := 'F_COLLAPSE_FNLNE_STAGE' ;
    V_ERR_CODE   VARCHAR2 ( 10 ) ;
    V_ERR_MSG    VARCHAR2 ( 255 ) ;
    V_CREATED_ON TIMESTAMP ( 6 ) ;
  BEGIN
    INSERT
      /*+ PARALLEL(AUTO) APPEND */
    INTO F_COLLAPSE_FNLNE_STAGE
      (
        F_COLLAPSE_FNLNE_STAGE_ID,
        SUSPECT_GROUP_ID,
        SUSPECT_CONT_ID,
        CONT_ID,
        CREATED_DT,
        SUSPECT_GROUP_COUNT,
        SUSPECT_ROWNUM
      )
    SELECT
      /*+ PARALLEL(AUTO) */
      ROWNUM AS F_COLLAPSE_FNLNE_STAGE_ID,
      Q.SUSPECT_GROUP_ID,
      Q.SUSPECT_CONT_ID,
      Q.CONT_ID,
      Q.CREATED_DT,
      COUNT ( Q.SUSPECT_GROUP_ID ) OVER ( PARTITION BY Q.SUSPECT_GROUP_ID )              AS SUSPECT_GROUP_COUNT, /*+ This count will be used for a custom filter in the insert to FORCE_COLLAPSE */
      ROW_NUMBER ( ) OVER ( PARTITION BY Q.SUSPECT_GROUP_ID ORDER BY Q.CREATED_DT DESC ) AS SUSPECT_ROWNUM /*+ Order merge recursions by created date descending */
    FROM
      (
        SELECT Q.SUSPECT_GROUP_ID,
          MAX ( Q.SUSPECT_CONT_ID ) OVER ( PARTITION BY Q.SUSPECT_GROUP_ID ) AS SUSPECT_CONT_ID, /*+ The MAX function will select the SUSPECT_CONT_ID for the SUSPECT_GROUP_ID partition */
          Q.CONT_ID,
          Q.CREATED_DT
        FROM /*+ Split the CONT_ID from F_COLLAPSE_FNLNE_MATCH into seprate columns by transposing with CASE */
          (
            SELECT FC.SUSPECT_GROUP_ID,
              CASE
                WHEN FC.SUSPECT_IND = 'Y'
                THEN FC.CONT_ID
              END AS SUSPECT_CONT_ID, /*+ Create SUSPECT_CONT_ID for suspects */
              CASE
                WHEN FC.SUSPECT_IND = 'N'
                THEN FC.CONT_ID
              END AS CONT_ID, /*+ Create CONT_ID for non-suspects */
              CASE /*+ The non-suspect created date will be used for ordering merge recursisions, where newer guests merge first */
                WHEN FC.SUSPECT_IND = 'N'
                THEN C.CREATED_DT
              END AS CREATED_DT
            FROM F_COLLAPSE_FNLNE_MATCH FC
            INNER JOIN MDMMGR.CONTACT C
            ON FC.CONT_ID = C.CONT_ID
          )
          Q
      )
      Q
    WHERE Q.CONT_ID IS NOT NULL /*+ Remove nulls from the MAX function */
    AND EXISTS /*+ Filter for CONT_IDS that have a unique combination of attributes */
      (
        SELECT 1
        FROM F_COLLAPSE_FNLNE_MINATTRIB FMA
        WHERE Q.CONT_ID = FMA.CONT_ID
      ) ;
    COMMIT;
    EXCEPTION /*+ Log errors in F_COLLAPSE_FNLNE_ERRLOG if execptions occur*/
    WHEN OTHERS THEN
      V_TABLE_NAME := V_TABLE_NAME;
      V_ERR_CODE   := SQLCODE;
      V_ERR_MSG    := SUBSTR ( SQLERRM, 1, 255 ) ;
      V_CREATED_ON := SYSTIMESTAMP;
      INSERT
        /*+ APPEND */
      INTO F_COLLAPSE_FNLNE_ERRLOG
        (
          TABLE_NAME,
          ERR_CODE,
          ERR_MSG,
          CREATED_ON
        )
        VALUES
        (
          V_TABLE_NAME,
          V_ERR_CODE,
          V_ERR_MSG,
          V_CREATED_ON
        ) ;
  END FORCE_COLLAPSE_FNLNE_STAGE;
END FORCE_COLLAPSE_FNLNE;