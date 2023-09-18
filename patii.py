from datetime import timedelta, date, datetime
from sqtoolbox.utility.utilities import token_gen
from sqtoolbox.data.connection import DataSourceConnection
from sqtoolbox.data.utilities import is_table_exist, get_latest_spe_wks, drop_if_exist, number_of_rows
from sqtoolbox.gen.sqconstants import SQConstants
from sqtoolbox.gen.sqlogging import SQLogger, SQLoggerM
from project_constants import projConst
from ddls import just_ddls

class PATII(object):

    def __init__(self, cur,cur_spe):
        """initialization, and set the properties of start/end dates.

        """
        super().__init__()
        self.cur = cur
        self.cur_spe = cur_spe
        self.item_master_tb = 'ITEM_MASTER' 
        self.check_records_tb = 'CHECK_RECORDS' 
        self.multi_item_pin_tb = 'MULTI_ITEM_PIN'
        self.not_in_sq_fct = 'PIN_NOT_IN_FCT'
        self.fyfw = 0
        self.key_column = ''
        self.edw_key_column = ''        
        self.pat_scan_tb = ''
        self.pat_missort_tb = ''

    def keep_wip_vtb(self, source_tb, output_tb, pi):
        # for development/debugging purpose only

        drop_if_exist(self.cur, output_tb)
        self.cur.execute(f"""
            CREATE MULTISET TABLE {output_tb} AS (
                SEL * FROM {source_tb}
            ) WITH DATA PRIMARY INDEX ({pi});
        """)
        
    def query_runner(self, queries : list) -> None:
        """this method is used by other methods to run a sequence of queries
        and show log information accordingly. 

        :param queries: a serious queries which are written in a formated dictionary like below
            {
                'description_of_the_query' :  'query_statemnet',
                'description_of_the_query' :  'query_statemnet',
                ...
            }
            where, 
                description_of_the_query: a brief string to tell the purpose of this query 
                    and this string will be shown in logs when running the query. 
                    there is some special codes can be added for special usage,

                    '!!!' added at the begining of the description text will do a explain 
                    with helpstats before running the query. if it is DDL with SELECT 
                    (e.g. CREATE TABLE... AS (SELECT...) WITH DATA )
                    only the inside SELECT statement will be extracted and explained. 
                    
                query_statement: the string of query statement need to be run. 

        :type queries: dict
        """

        for desc, q in queries.items():
            if desc[0:3] == "***":
                # for queries which get a number the out put the number in log
                self.cur.execute(q)
                n = self.cur.fetchone()[0]
                SQLoggerM(
                    f"{desc} is: {n}",
                    log_filename=projConst.LOG_FILE,
                    proj_desc=projConst.PROJECT_NAME,
                    proj_id=projConst.PROJECT_NO,
                )
            elif desc[0:3] == "!!!":
                SQLoggerM(
                    f"Explain {desc}",
                    log_filename=projConst.LOG_FILE,
                    proj_desc=projConst.PROJECT_NAME,
                    proj_id=projConst.PROJECT_NO,
                )
                self.cur.execute('DIAGNOSTIC HELPSTATS ON FOR SESSION')
                ddl_prefix = q.split('(')[0] + '('
                ddl_surfix = ')' + q.split(')')[-2] + ')' + q.split(')')[-1]
                remove_pre = q.replace(ddl_prefix, '')
                remove_surfix = remove_pre.replace(ddl_surfix, '')
                self.cur.execute('EXPLAIN ' + remove_surfix)
                explain_suggestion = self.cur.fetchall()
                output = '\n'.join([e[0] for e in explain_suggestion])
                SQLoggerM(output,
                    log_filename=projConst.LOG_FILE,
                    proj_desc=projConst.PROJECT_NAME,
                    proj_id=projConst.PROJECT_NO
                )
                SQLoggerM(f'Running {desc}',
                    log_filename=projConst.LOG_FILE,
                    proj_desc=projConst.PROJECT_NAME,
                    proj_id=projConst.PROJECT_NO
                )
                self.cur.execute(q)
            elif desc[0:3] == "###": # get number of rows of the table name specified in desc after + sign.
                SQLoggerM(f'Running {desc}',
                    log_filename=projConst.LOG_FILE,
                    proj_desc=projConst.PROJECT_NAME,
                    proj_id=projConst.PROJECT_NO
                )
                self.cur.execute(q)
                tb_name_pos = desc.find('+')
                if tb_name_pos >= 0:
                    tb_name = desc[tb_name_pos+1:]
                    r = number_of_rows(self.cur, tb_name)
                    SQLoggerM(f'{r} rows in table {tb_name}.',
                        log_filename=projConst.LOG_FILE,
                        proj_desc=projConst.PROJECT_NAME,
                        proj_id=projConst.PROJECT_NO
                    )   
                SQLoggerM(
                    f"Finished query {desc}",
                    log_filename=projConst.LOG_FILE,
                    proj_desc=projConst.PROJECT_NAME,
                    proj_id=projConst.PROJECT_NO,
                )
            else:
                SQLoggerM(
                    f"Start to run query {desc}",
                    log_filename=projConst.LOG_FILE,
                    proj_desc=projConst.PROJECT_NAME,
                    proj_id=projConst.PROJECT_NO,
                )
                self.cur.execute(q)
                SQLoggerM(
                    f"Finished query {desc}",
                    log_filename=projConst.LOG_FILE,
                    proj_desc=projConst.PROJECT_NAME,
                    proj_id=projConst.PROJECT_NO,
                )
    def drop_multiple_tables(self, tables_to_drop : list):
        """drop table(s) listed in tables_to_drop list if exists. 

        :param tables_to_drop: name(s) of table(s) to be dropped
        :type tables_to_drop: list
        """ 

        for t in tables_to_drop:
            sql = f"""
            Drop table {t}
            """
            self.cur.execute(sql)
            # __, msg = drop_if_exist(self.cur, t)
            msg=f"{t} doesn't exist."

            SQLoggerM(
                msg,
                log_filename=projConst.LOG_FILE,
                proj_desc=projConst.PROJECT_NAME,
                proj_id=projConst.PROJECT_NO,
            )
            
    # get item and related attributes.         
    def spe_get_next_fyfw(self, start_fyfw : int, next_no_of_wks : int) -> int:
        """return fiscal year week number based on the start week and number 
        of weeks before/after it

        :param start_fyfw: start fiscal year week number, e.g. 202252
        :type start_fyfw: int
        :param next_no_of_wks: number of weeks before (minors number) 
            or after (positive number) start_fyfw
        :type next_no_of_wks: int
        :return: fiscal year fiscal week number before/after start_fyfw. 
            returns none if the fyfw is not availabe in data base. 
        :rtype: int
        """        
        
        sql = f"""
            SEL TARGET_WK.FISCAL_YEAR_WEEK_NUMBER
            FROM {SQConstants.SQ_WEEK_VW} START_WK
            INNER JOIN {SQConstants.SQ_WEEK_VW} TARGET_WK
                ON START_WK.FISCAL_WEEK_TO_CURRENT + {next_no_of_wks} = TARGET_WK.FISCAL_WEEK_TO_CURRENT
                    AND START_WK.FISCAL_YEAR_WEEK_NUMBER = {start_fyfw}
        """
        
        self.cur.execute(sql)
        fyfw = self.cur.fetchone()
        return fyfw[0] if len(fyfw) == 1 else None
    
    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def spe_prepare_items(self):
        """collect required attributes from SPE and SQ FCT_ITEM table. 
        """        
      
        qs = {
            "get_unique_spdm_items" : f"""
                CREATE MULTISET VOLATILE TABLE UNIQUE_SPE_ITEM AS (
                    SEL ITEM, PRODUCT
                        
                        --CUDT and CUTM are for GDE items. 
                        , CASE TRIM(IND_SRC)
                            WHEN 'CUS' THEN CUDT
                            ELSE ODT
                        END ODT
                        , CASE TRIM(IND_SRC)
                            WHEN 'CUS' THEN CUTM
                            ELSE OTM
                        END OTM
                        
                        , OCUTOFFT, ONTIME DP0, DP1,
                        REGEXP_SUBSTR(UPPER(TRIM(OFSA)), '{projConst.FSA_REG_EXP}') OFSA, 
                        REGEXP_SUBSTR(UPPER(TRIM(OLDU)), '{projConst.LDU_REG_EXP}') OLDU, 
                        REGEXP_SUBSTR(UPPER(TRIM(DFSA)), '{projConst.FSA_REG_EXP}') DFSA, 
                        REGEXP_SUBSTR(UPPER(TRIM(DLDU)), '{projConst.LDU_REG_EXP}') DLDU, 
                        DCNTR, OPROV, DPROV, STD, VALID, CAST(IND_SRC AS VARCHAR(25)) IND_SRC,
                        DECODE(TRIM(IND_SRC), 'PU', PURC, 'RVU', RVURC, 'CUS', CURC, ORC) IND_WC,
                        CASE TRIM(IND_SRC)
                            WHEN 'PU' THEN CAST(PUDT AS TIMESTAMP(0)) + (PUTM - TIME '00:00:00' HOUR TO SECOND)
                            WHEN 'RVU' THEN CAST(RVUDT AS TIMESTAMP(0)) + (RVUTM - TIME '00:00:00' HOUR TO SECOND)
                            WHEN 'CUS' THEN CAST(CUDT AS TIMESTAMP(0)) + (CUTM - TIME '00:00:00' HOUR TO SECOND)
                            ELSE CAST(ODT AS TIMESTAMP(0)) + (OTM - TIME '00:00:00' HOUR TO SECOND)
                        END IND_DTM,
                        'DOMESTIC' SPE_DATA_TYPE
                    FROM {projConst.SPDM_DOMESTIC}
                    WHERE FYFW = {self.fyfw} AND SPE_DUP_CODE < 2
                    
                    UNION
                    
                    SEL ITEM, PRODUCT, ODT, OTM, OCUTOFFT, ONTIME DP0, DP1,
                        REGEXP_SUBSTR(UPPER(TRIM(OFSA)), '{projConst.FSA_REG_EXP}') OFSA, 
                        REGEXP_SUBSTR(UPPER(TRIM(OLDU)), '{projConst.LDU_REG_EXP}') OLDU, 
                        REGEXP_SUBSTR(UPPER(TRIM(DFSA)), '{projConst.FSA_REG_EXP}') DFSA,
                        REGEXP_SUBSTR(UPPER(TRIM(DLDU)), '{projConst.LDU_REG_EXP}') DLDU, 
                        DCNTR, OPROV, DPROV, STD, VALID,  'INBOUND',
                        ORC IND_WC, CAST(ODT AS TIMESTAMP(0)) + (OTM - TIME '00:00:00' HOUR TO SECOND) IND_DTM, 
                        'INBOUND'
                        
                    FROM {projConst.SPDM_INBOUND}
                    WHERE FYFW = {self.fyfw} AND SPE_DUP_CODE < 2
                    
                    UNION
                    
                    SEL ITEM, LEFT(PRODUCT, 2), ODT, OTM, OCUTOFFT, OBOT DP0, OBDP1 DP1,
                        REGEXP_SUBSTR(UPPER(TRIM(OFSA)), '{projConst.FSA_REG_EXP}') OFSA, 
                        REGEXP_SUBSTR(UPPER(TRIM(OLDU)), '{projConst.LDU_REG_EXP}') OLDU, 
                        REGEXP_SUBSTR(UPPER(TRIM(OBFSA)), '{projConst.FSA_REG_EXP}') DFSA, 
                        REGEXP_SUBSTR(UPPER(TRIM(OBLDU)), '{projConst.LDU_REG_EXP}') DLDU
                        , OBCNTR DCNTR, OPROV, DPROV, OBSTD, OBVALID, IND_SRC,
                        COALESCE(RVURC, ORC) IND_WC,                          --OUTBOUND table has no PURC
                        /*  RVUTM in spe outbound table is broken, can only use ODT/OTM now
                        COALESCE(
                             
                            CAST(RVUDT AS TIMESTAMP(0)) + (RVUTM - TIME '00:00:00' HOUR TO SECOND)  -- RVUTM in spe outbound table is broken
                            , CAST(ODT AS TIMESTAMP(0)) + (OTM - TIME '00:00:00' HOUR TO SECOND)
                        ) IND_DTM,
                        */
                        CAST(ODT AS TIMESTAMP(0)) + (OTM - TIME '00:00:00' HOUR TO SECOND) IND_DTM,
                        'OUTBOUND'
                    FROM {projConst.SPDM_OUTBOUND}
                    WHERE FYFW = {self.fyfw} AND SPE_DUP_CODE < 2
                ) WITH DATA UNIQUE PRIMARY INDEX (ITEM)
                ON COMMIT PRESERVE ROWS;
            """,
            
            "get_other_info_from_sq" : f"""
                CREATE MULTISET VOLATILE TABLE {self.item_master_tb} AS (
                    SEL T1.ITEM PIN
                        , T2.ITEM_ID
                        , T1.PRODUCT, T1.ODT, T1.OTM, T1.OCUTOFFT, T1.OPROV
                        , T1.DPROV, T1.DCNTR, T1.STD, T1.VALID, T1.SPE_DATA_TYPE, T1.DP0, T1.DP1
                        , T1.IND_SRC, T1.IND_WC, T1.IND_DTM
                        , COALESCE(T1.OFSA, T2.OFSA) OFSA
                        , COALESCE(T1.OLDU, T2.OLDU) OLDU
                        , CASE T2.RETURN_FLG 
                            WHEN 'T' THEN T2.SPECIAL_DFSA_DZIP
                            ELSE COALESCE(T1.DFSA, T2.DFSA)
                        END DFSA
                        , COALESCE(T1.DLDU, T2.DLDU) DLDU
                        , REGEXP_SUBSTR(T2.SPECIAL_DFSA_DZIP, '^\d\d\d') OUTBOUND_US_ZIP
                        
                        , T2.BYPASS_WC
                        , T2.DDI_FLG
                        , T2.RETURN_FLG
                        , T2.OUTBOUND_FLG
                        , COALESCE(T3.CATEGORY_CODE, 'EP') PRODUCT_CATEGORY
                        
                        --receipt
                        , T2.RECEIPT_TYPE_CODE
                        , T2.RECEIPT_SCAN_CODE
                        , T2.RECEIPT_SCAN_WC
                        , T2.RECEIPT_SCAN_DTM_LOC
                        , T2.RECEIPT_SCAN_DTM_UTC
                        , T2.RECEIPT_CUTOFF
                        , T2.RECEIPT_ROLLUP_SITE
                        
                        --manifest
                        , T2.MANIFEST_SCAN_WC
                        , T2.MANIFEST_SCAN_CODE
                        , T2.MANIFEST_SCAN_DTM_LOC
                        , T2.MANIFEST_SCAN_DTM_UTC

                        --first plt first ilc
                        , T2.LOGIC_F_PLT_PROC_SITE
                        , T2.F_PLT_F_PROC_WC
                        , T2.F_PLT_F_PROC_SITE
                        , T2.F_PLT_F_PROC_DTM_LOC
                        , T2.F_PLT_F_PROC_DTM_UTC
                        
                        --first plt last ilc
                        , T2.F_PLT_L_PROC_WC
                        , T2.F_PLT_L_PROC_DTM_LOC
                        , T2.F_PLT_L_PROC_DTM_UTC
                        
                        --last plt first ilc
                        , T2.LOGIC_L_PLT_PROC_SITE 
                        
                        --last plt first ilc
                        , T2.L_PLT_F_PROC_WC
                        , T2.L_PLT_F_PROC_SITE
                        , T2.L_PLT_F_PROC_DTM_LOC
                        , T2.L_PLT_F_PROC_DTM_UTC
                        
                        --last plt last ilc
                        , T2.L_PLT_L_PROC_WC
                        , T2.L_PLT_L_PROC_DTM_LOC
                        , T2.L_PLT_L_PROC_DTM_UTC
                        
                        --first dss scan at last depot
                        , T2.L_DEPOT_F_DSS_WC
                        , T2.L_DEPOT_F_DSS_DTM_LOC
                        , T2.L_DEPOT_F_DSS_DTM_UTC
                        , T2.L_DEPOT_F_DSS_SCAN_CODE
                        
                        --first delivery/leg 1 end scan
                        , T2.F_ATT_DEL_WC
                        , T2.F_ATT_DEL_SCAN_CODE
                        , T2.F_ATT_DEL_DTM_LOC
                        , T2.F_ATT_DEL_DTM_UTC
                        
                        , CAST(NULL AS DATE) CLOCK_START_DT
                        , CAST(NULL AS TIMESTAMP(0)) CLOCK_START_DTM --created for oplt cutoff time table query
                        , CAST(NULL AS DATE) STD_DELIVER_DATE_DP0
                        , CAST(NULL AS DATE) STD_DELIVER_DATE_DP1
                        , CAST(NULL AS SMALLINT) DP0_CAT_ID
                        , CAST(NULL AS SMALLINT) DP1_CAT_ID
                        , CAST(NULL AS SMALLINT)  AS DOW
                        
                    FROM UNIQUE_SPE_ITEM T1
                    LEFT JOIN {projConst.FCT_ITEM} T2
                        ON T1.ITEM = T2.PIN
                    LEFT JOIN {projConst.SQ_SPE_PRODUCT_DIM} T3
                        ON COALESCE(T1.PRODUCT, 'EP') = T3.PRODUCT_CODE
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY T1.ITEM
                        ORDER BY ABS(T1.ODT - CAST(T2.RECEIPT_SCAN_DTM_LOC AS DATE)) ASC
                    ) = 1
                ) WITH DATA PRIMARY INDEX ({self.key_column})
                ON COMMIT PRESERVE ROWS;
            """,    

            #    add STATISTICS FOR QUERY OPTIMIZATOIN
            'calculate statistics for ITEM_MASTER' : f"""
                COLLECT STATISTICS 
                    COLUMN (DFSA),
                    COLUMN (OFSA),
                    COLUMN (ITEM_ID),
                    COLUMN (PRODUCT_CATEGORY ,F_PLT_L_PROC_WC),
                    COLUMN (PRODUCT_CATEGORY ,F_PLT_L_PROC_WC, DOW)
                ON {self.item_master_tb};
            """, 
                 
            # deal with the situation when one pin has multiple ITEM_ID 
            # associated to, or have to item_id found. 
         
            "delete_pin_from_master_if_exist" : f"""
                DELETE {projConst.FCT_PAT_SPDM_SCAN}
                FROM {self.item_master_tb} T
                WHERE {projConst.FCT_PAT_SPDM_SCAN}.PIN = T.PIN
                    AND T.ITEM_ID IS NULL
                    AND {projConst.FCT_PAT_SPDM_SCAN}.FYFW = {self.fyfw}
            """,
            
            "insert_pins_which_has_no_item_ids" : f"""
                INSERT INTO {projConst.FCT_PAT_SPDM_SCAN}
                SEL T.PIN
                    , NULL
                    , {projConst.PAT_CAT_SEQ['INDUCTION']}
                    , NULL
                    , 'UN'
                    , 19
                    , NULL , NULL, NULL, 1, 1, NULL
                    , -1 * {projConst.PAT_CAT_SEQ['INDUCTION']}
                    , -1 * {projConst.PAT_CAT_SEQ['INDUCTION']}
                    , {self.fyfw}
                    , CAST(CURRENT_TIMESTAMP(0) AS TIMESTAMP(0)) 
                FROM {self.item_master_tb} T
                WHERE T.ITEM_ID IS NULL
            """,
            
            "remove_non_item_id_pins" : f'DEL FROM {self.item_master_tb} WHERE ITEM_ID IS NULL',
            
            'collect_stats_on_item_master' : f"""
                COLLECT STATISTICS 
                    COLUMN (ITEM_ID)
                    , COLUMN ({self.key_column})
                    , COLUMN (PRODUCT_CATEGORY ,F_PLT_L_PROC_WC)
                    , COLUMN (OFSA)
                    , COLUMN (CLOCK_START_DT)
                    , COLUMN (DCNTR)
                    , COLUMN (DFSA)
                    , COLUMN (PRODUCT_CATEGORY ,L_PLT_L_PROC_WC)
                    , COLUMN (BYPASS_WC)
                ON {self.item_master_tb}
            """,
            
            'drop_temp_table' : 'DROP TABLE UNIQUE_SPE_ITEM'
        }
        
        self.query_runner(qs)
    
    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def flash_extract(self):
        sql = f"""
                CREATE MULTISET TABLE DL_SQ_PRD_WRK.DAILY_FLASH AS 
                (sel item AS PIN, OPROV,DPROV,PRODUCT,STD,ODT,COMMITDT from SPEDB.DAILY_FLASH_VW)
                WITH DATA PRIMARY INDEX (PIN,PRODUCT,ODT);
            """
        self.cur_spe.execute(sql)

        sql1 = f"""
                CREATE MULTISET VOLATILE TABLE DAILY_FLASH_NEW AS (
                SEL * FROM DL_SQ_PRD_WRK.DAILY_FLASH
                ) WITH DATA UNIQUE PRIMARY INDEX (PIN,PRODUCT,ODT)
                ON COMMIT PRESERVE ROWS;
            """
        self.cur.execute(sql1)
        
        sql2 = f"""
                DROP TABLE DL_SQ_PRD_WRK.DAILY_FLASH;
            """
        self.cur.execute(sql2)

        sql3 = f"""
                    CREATE MULTISET VOLATILE Table CLOCK_SPE AS (with AB AS (
                    sel SPEF.*,
                    CASE
                    WHEN BUSDAY.BUSINESS_DATE_FLG='F' THEN busday.NEXT_BUSINESS_DATE
                    ELSE BUSDAY.CALENDAR_DATE
                    END AS SPE_ODT
                    FROM DAILY_FLASH_NEW SPEF
                    INNER JOIN DL_SQ_PRD_INT.VW_LKP_NEXT_PREV_BUSDAY_V2 BUSDAY
                    ON SPEF.ODT = BUSDAY.CALENDAR_DATE
                    AND COALESCE(SPEF.OPROV, 'ON') = BUSDAY.PROV_CODE
                    )
                    SEL OI.ITEM_ID, OI.PIN,AB.OPROV,AB.DPROV,AB.PRODUCT,AB.ODT,
                    COALESCE (OI.SPE_CLOCK_START_DT,AB.SPE_ODT ) AS SPE_CLOCK_START_DT
                    FROM DL_SQ_PRD_INT.VW_FCT_SPE_CLOCK_START OI LEFT JOIN AB
                    on OI.PIN=AB.PIN
                    AND OI.PRODUCT = AB.PRODUCT
                    AND OI.ODT = AB.ODT)
                    WITH DATA PRIMARY INDEX (ITEM_ID, PIN)
                    ON COMMIT PRESERVE ROWS;
                """
        self.cur.execute(sql3)



    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def spe_extract(self,fyfw):
        """ extract ORIGN_DATE from table SPE_SEM_VW.SQS_COMBINED
        """
        sql = f"""
                CREATE MULTISET TABLE DL_SQ_PRD_WRK.FCT_PAT_ORIGIN_DT as (
                sel ITEM_ID AS PIN,ORIGN_DATE,PRODUCT_DESC,ORIGN_PROV from SPE_SEM_VW.SQS_COMBINED where ORIGN_FISC_WEEK ='{fyfw}'
                UNION
                sel ITEM_ID AS PIN,ORIGN_DATE,PRODUCT_DESC, 'ON' as ORIGN_PROV from SPE_SEM_VW.SQS_INBOUND where ORIGN_FISC_WEEK ='{fyfw}'
                UNION
                sel ITEM_ID AS PIN,ORIGN_DATE,PRODUCT_DESC , 'ON' as ORIGN_PROV from SPE_SEM_VW.SQS_OUTBOUND where ORIGN_FISC_WEEK ='{fyfw}'
                QUALIFY ROW_NUMBER() OVER (
                PARTITION BY PIN,PRODUCT_DESC
                ORDER BY ORIGN_DATE DESC) = 1
                )
                WITH DATA PRIMARY INDEX (PIN,PRODUCT_DESC,ORIGN_DATE);
            """
        self.cur.execute(sql)

        sql1 = f"""
                CREATE MULTISET TABLE DL_SQ_PRD_WRK.SPE_PRODUCT as (
                sel * from SPEDB.PROD_EN_DIM ) 
                WITH DATA PRIMARY INDEX (PROD);
            """
        self.cur_spe.execute(sql1)
        
        sql2 = f"""
                CREATE MULTISET VOLATILE TABLE FCT_PAT_ORIGIN_DT as (
                sel A.*,B.PROD,C.ITEM_ID from DL_SQ_PRD_WRK.FCT_PAT_ORIGIN_DT A
                INNER JOIN DL_SQ_PRD_WRK.SPE_PRODUCT B
                ON A.PRODUCT_DESC=B.PROD_DESC
                INNER JOIN {projConst.FCT_ITEM} C
                ON A.PIN = C.PIN)
                WITH DATA PRIMARY INDEX (ITEM_ID,PIN)
                ON COMMIT PRESERVE ROWS;
            """
        self.cur.execute(sql2)

        sql3 = f"""
                DROP TABLE DL_SQ_PRD_WRK.FCT_PAT_ORIGIN_DT;
            """
        self.cur.execute(sql3)

        sql4 = f"""
                DROP TABLE DL_SQ_PRD_WRK.SPE_PRODUCT;
            """
        self.cur.execute(sql4)
       
    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def flash_prepare_items(self):
        """collect attributes from spe/sq fct item. 
        """        
        qs = {
            "collect_items_delivered_in_past_days" : f"""
                CREATE MULTISET VOLATILE TABLE {self.item_master_tb} AS (
                    SEL  ITEMS.PIN
                        , ITEMS.ITEM_ID
                        , ITEMS.PRODUCT 
                        , CAST(NULL AS BYTEINT) DP0
                        , CAST(NULL AS BYTEINT) DP1
                        , OWC.SITE_PROVINCE_CODE OPROV
                        , DWC.SITE_PROVINCE_CODE DPROV
                        , ITEMS.DCNTR 
                        , ITEMS.SPECIAL_DFSA_DZIP
                        , ITEMS.SPE_DATA_TYPE
                        , ITEMS.OFSA
                        , DECODE(ITEMS.RETURN_FLG, 'T', ITEMS.SPECIAL_DFSA_DZIP, ITEMS.DFSA) DFSA
                        , REGEXP_SUBSTR(ITEMS.SPECIAL_DFSA_DZIP, '^\d\d\d') OUTBOUND_US_ZIP
                        , ITEMS.BYPASS_WC
                        , ITEMS.DDI_FLG
                        , ITEMS.RETURN_FLG
                        , ITEMS.OUTBOUND_FLG
                        , COALESCE(PRD.CATEGORY_CODE, 'EP') PRODUCT_CATEGORY--receipt
                        , ITEMS.RECEIPT_TYPE_CODE
                        , ITEMS.RECEIPT_SCAN_CODE
                        , ITEMS.RECEIPT_SCAN_WC
                        , ITEMS.RECEIPT_SCAN_DTM_LOC
                        , ITEMS.RECEIPT_SCAN_DTM_UTC
                        , ITEMS.RECEIPT_CUTOFF
                        , ITEMS.RECEIPT_ROLLUP_SITE
                        
                        --manifest
                        , ITEMS.MANIFEST_SCAN_WC
                        , ITEMS.MANIFEST_SCAN_CODE
                        , ITEMS.MANIFEST_SCAN_DTM_LOC
                        , ITEMS.MANIFEST_SCAN_DTM_UTC
                        
                        --first plt first ilc
                        , ITEMS.LOGIC_F_PLT_PROC_SITE
                        , ITEMS.F_PLT_F_PROC_WC
                        , ITEMS.F_PLT_F_PROC_SITE
                        , ITEMS.F_PLT_F_PROC_DTM_LOC
                        , ITEMS.F_PLT_F_PROC_DTM_UTC

                        --first plt last ilc
                        , ITEMS.F_PLT_L_PROC_WC
                        , ITEMS.F_PLT_L_PROC_DTM_LOC
                        , ITEMS.F_PLT_L_PROC_DTM_UTC
                        
                        , ITEMS.LOGIC_L_PLT_PROC_SITE 

                        --last plt first ilc
                        , ITEMS.L_PLT_F_PROC_WC
                        , ITEMS.L_PLT_F_PROC_SITE
                        , ITEMS.L_PLT_F_PROC_DTM_LOC
                        , ITEMS.L_PLT_F_PROC_DTM_UTC

                        --last plt last ilc
                        , ITEMS.L_PLT_L_PROC_WC
                        , ITEMS.L_PLT_L_PROC_DTM_LOC
                        , ITEMS.L_PLT_L_PROC_DTM_UTC

                        --first dss scan at last depot
                        , ITEMS.L_DEPOT_F_DSS_WC
                        , ITEMS.L_DEPOT_F_DSS_DTM_LOC
                        , ITEMS.L_DEPOT_F_DSS_DTM_UTC
                        , ITEMS.L_DEPOT_F_DSS_SCAN_CODE

                        --first delivery/leg 1 end scan
                        , ITEMS.F_ATT_DEL_WC
                        , ITEMS.F_ATT_DEL_SCAN_CODE
                        , ITEMS.F_ATT_DEL_DTM_LOC
                        , ITEMS.F_ATT_DEL_DTM_UTC

                        , COALESCE (SPE_CLOCK.SPE_CLOCK_START_DT,ITEMS.CLOCK_START_DT) CLOCK_START_DT             --DF.ODT_RECALCULATED as CLOCK_START_DT ,ITEMS.CLOCK_START_DT,SPE_CLOCK.SPE_CLOCK_START_DT CLOCK_START_DT 
                        , CAST(COALESCE(SPE_CLOCK.SPE_CLOCK_START_DT,ITEMS.CLOCK_START_DT) AS TIMESTAMP(0)) CLOCK_START_DTM --created for oplt cutoff time table query CAST(ITEMS.CLOCK_START_DT AS TIMESTAMP(0))
                        , CAST(NULL AS DATE) STD_DELIVER_DATE_DP0 -- ITEMS.ESTIMATED_DELIVERY_DT,DF.COMMITDT change back when service team fix it.
                        , CAST(NULL AS DATE) STD_DELIVER_DATE_DP1
                        , CAST(NULL AS SMALLINT) DP0_CAT_ID
                        , CAST(NULL AS SMALLINT) DP1_CAT_ID
                        , TD_DAY_OF_WEEK(COALESCE(SPE_CLOCK.SPE_CLOCK_START_DT,ITEMS.CLOCK_START_DT))  AS DOW -- query optimizer for oplt cutoff 
                        , ITEMS.SPE_ODT -- TEST 2023/08/24
                    FROM {projConst.FCT_ITEM} ITEMS
                    LEFT JOIN CLOCK_SPE SPE_CLOCK
                    ON ITEMS.PIN = SPE_CLOCK.PIN
                    AND ITEMS.ITEM_ID = SPE_CLOCK.ITEM_ID
                    LEFT JOIN {projConst.SQ_SPE_PRODUCT_DIM} PRD
                        ON COALESCE(ITEMS.PRODUCT, 'EP') = PRD.PRODUCT_CODE
                    LEFT JOIN {SQConstants.SQ_WORKCENTRE_DIM} OWC
                        ON ITEMS.RECEIPT_SCAN_WC = OWC.WORK_CENTRE_ID
                    LEFT JOIN {SQConstants.SQ_WORKCENTRE_DIM} DWC
                        ON ITEMS.F_ATT_DEL_WC = DWC.WORK_CENTRE_ID
                    WHERE COALESCE (SPE_CLOCK.SPE_CLOCK_START_DT,ITEMS.CLOCK_START_DT) BETWEEN DATE - {projConst.FLASH_DAYS_RANGE} AND DATE
                ) WITH DATA UNIQUE PRIMARY INDEX (ITEM_ID)
                ON COMMIT PRESERVE ROWS;
            """,
            #    add STATISTICS FOR QUERY OPTIMIZATOIN
            'calculate statistics for ITEM_MASTER' : f"""
                COLLECT STATISTICS 
                    COLUMN (DFSA),
                    COLUMN (OFSA),
                    COLUMN (ITEM_ID),
                    COLUMN (PRODUCT_CATEGORY ,F_PLT_L_PROC_WC),
                    COLUMN (PRODUCT_CATEGORY ,F_PLT_L_PROC_WC, DOW)
                ON {self.item_master_tb};
            """,
            
            'calculate cal_std_delivery_date_dp0_dp1' : f"""
                CREATE MULTISET VOLATILE TABLE STD_DEL AS (
                    SEL ITEMS.PIN,ITEMS.ITEM_ID,ITEMS.PRODUCT,ITEMS.OPROV,ITEMS.SPE_ODT
                        ,DURATION_DP0.DDATE STD_DELIVER_DATE_DP0
                         ,DURATION_DP1.DDATE  STD_DELIVER_DATE_DP1 
                    FROM {self.item_master_tb} ITEMS
                    INNER JOIN PARCEL_SEM_VW.ITEM_SMRY SEM_VW
                    ON ITEMS.ITEM_ID = SEM_VW.ITEM_ID
                    AND ITEMS.PIN = SEM_VW.ASSOCIATED_PIN
                    LEFT JOIN DAILY_FLASH_NEW DF 
                    ON DF.PIN = ITEMS.PIN AND
                    DF.PRODUCT = ITEMS.PRODUCT And
                    DF.ODT = ITEMS.SPE_ODT -- TEST 2023/08/24
                    LEFT JOIN DL_SQ_PRD_INT.VW_LKP_DURATION DURATION_DP0
                        ON ITEMS.CLOCK_START_DT = DURATION_DP0.ODATE 
                            AND ITEMS.OPROV = DURATION_DP0.OPROV
                            AND ITEMS.DPROV = DURATION_DP0.DPROV
                            AND COALESCE(SEM_VW.SERVICE_STANDARD_CODE,DF.STD) = DURATION_DP0.DURATION             
                    LEFT JOIN DL_SQ_PRD_INT.VW_LKP_DURATION DURATION_DP1
                        ON ITEMS.CLOCK_START_DT = DURATION_DP1.ODATE
                            AND ITEMS.OPROV = DURATION_DP1.OPROV
                            AND ITEMS.DPROV = DURATION_DP1.DPROV
                            AND (COALESCE(SEM_VW.SERVICE_STANDARD_CODE,DF.STD) + 1) = DURATION_DP1.DURATION
                ) WITH DATA UNIQUE PRIMARY INDEX (ITEM_ID)
                ON COMMIT PRESERVE ROWS;
            """,

            'overwrite estimated_delivery_date from Daily flash view' : f"""
                CREATE MULTISET VOLATILE TABLE STD_DEL_OVERWRITE AS (
                SEL a.pin,a.item_id,a.std_deliver_date_dp0,a.std_deliver_date_dp1,b.commitdt as cal_std_deliver_date_dp0
                ,CASE
                WHEN BUSDAY.BUSINESS_DATE_FLG='F' THEN BUSDAY.NEXT_BUSINESS_DATE
                ELSE BUSDAY.CALENDAR_DATE
                END AS cal_std_deliver_date_dp1 
                FROM STD_DEL a LEFT join 
                DAILY_FLASH_NEW b 
                on a.pin=b.pin and a.product = b.product 
                INNER JOIN DL_SQ_PRD_INT.VW_LKP_NEXT_PREV_BUSDAY_V2 BUSDAY
                    ON (cal_std_deliver_date_dp0+1) = BUSDAY.CALENDAR_DATE
                    AND COALESCE(a.OPROV, 'ON') = BUSDAY.PROV_CODE)
                WITH DATA UNIQUE PRIMARY INDEX (ITEM_ID)
                ON COMMIT PRESERVE ROWS;
            """,

            'Merge the overwrite dates': f"""
                MERGE INTO STD_DEL T1
                USING STD_DEL_OVERWRITE T2
                    ON T1.item_id = T2.item_id
                    and T1.PIN = T2.PIN
                WHEN MATCHED THEN
                    UPDATE SET
                    STD_DELIVER_DATE_DP0 = T2.cal_std_deliver_date_dp0,
                    STD_DELIVER_DATE_DP1 = T2.cal_std_deliver_date_dp1;
            """
            ,

            'set and merge set_std_delivery_date_dp0_dp1' : f"""
             MERGE INTO {self.item_master_tb} T1
                USING STD_DEL T2
                    ON T1.item_id = T2.item_id
                WHEN MATCHED THEN
                    UPDATE SET
                    STD_DELIVER_DATE_DP0 = T2.STD_DELIVER_DATE_DP0,
                    STD_DELIVER_DATE_DP1 = T2.STD_DELIVER_DATE_DP1;
            """,
            'del old clock_start_dt for flash': f"""
            DEL FROM {projConst.PAT_FLASH_CLOCK_START_DT};
            """
            ,
            'get all clock_start_dt for flash' :f"""
            INSERT INTO {projConst.PAT_FLASH_CLOCK_START_DT}
            SEL ITEM_ID,PIN,RECEIPT_SCAN_DTM_LOC,OPROV,DPROV,CLOCK_START_DT,STD_DELIVER_DATE_DP0,CAST(CURRENT_TIMESTAMP(0) AS TIMESTAMP(0)) 
            FROM {self.item_master_tb};
            """,
            
            'set_dp0_dp1' : f"""
                UPDATE {self.item_master_tb}
                SET DP0 = CASE
                        WHEN F_ATT_DEL_DTM_LOC <= STD_DELIVER_DATE_DP0 THEN 1
                        WHEN F_ATT_DEL_DTM_LOC > STD_DELIVER_DATE_DP0 THEN 0
                        ELSE NULL
                    END    
                    , DP1 = CASE
                        WHEN F_ATT_DEL_DTM_LOC <= STD_DELIVER_DATE_DP1 THEN 1
                        WHEN F_ATT_DEL_DTM_LOC > STD_DELIVER_DATE_DP1 THEN 0
                        ELSE NULL
                    END
            """,
            'drop temp_tb' : 'DROP TABLE STD_DEL;',

            'drop table STD_DEL_OVERWRITE' : 'DROP TABLE STD_DEL_OVERWRITE;',
            
            }
        self.query_runner(qs)
     
    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def spe_clock_start_dt(self):
        """calculate clock start date for SPDM items. 
        """        
        qs = {
            
            'calculate_next_busi_day' : f"""
                CREATE MULTISET VOLATILE TABLE FCT_PAT_ORIGIN_DT_NEW AS (
                SEL A.*, 
                CASE
                WHEN BUSDAY.BUSINESS_DATE_FLG='F' THEN BUSDAY.NEXT_BUSINESS_DATE
                ELSE BUSDAY.CALENDAR_DATE
                END AS SPE_ODT, 
                COALESCE(B.SPE_CLOCK_START_DT,SPE_ODT) AS ORIGN_DATE_RECALCULATED 
                FROM FCT_PAT_ORIGIN_DT A
                INNER JOIN DL_SQ_PRD_INT.VW_FCT_SPE_CLOCK_START B
                ON A.PIN = B.PIN 
                AND A.ITEM_ID = B.ITEM_ID
                LEFT JOIN DL_SQ_PRD_INT.VW_LKP_NEXT_PREV_BUSDAY_V2 BUSDAY
                ON A.ORIGN_DATE = BUSDAY.CALENDAR_DATE
                AND COALESCE(A.ORIGN_PROV, 'ON') = BUSDAY.PROV_CODE
                ) WITH DATA UNIQUE PRIMARY INDEX (ITEM_ID,PIN,PROD)
                ON COMMIT PRESERVE ROWS;
            """,
            
            'merge_into_master' : f"""
                MERGE INTO {self.item_master_tb} T1
                USING FCT_PAT_ORIGIN_DT_NEW T2
                    on T1.ITEM_ID= T2.ITEM_ID
                    and T1.PIN=T2.PIN
                    and T1.PRODUCT=T2.PROD
                WHEN MATCHED THEN
                    UPDATE SET CLOCK_START_DT = T2.ORIGN_DATE_RECALCULATED
                    , DOW = TD_DAY_OF_WEEK(T2.ORIGN_DATE_RECALCULATED)
                    , CLOCK_START_DTM =  CAST(T2.ORIGN_DATE_RECALCULATED AS TIMESTAMP(0));
            """,

            'drop_temp_tb' : 'DROP TABLE FCT_PAT_ORIGIN_DT_NEW',
            
            'drop_temp_tb1' : 'DROP TABLE FCT_PAT_ORIGIN_DT'

            }
        self.query_runner(qs)
        
    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def spe_std_del_dt(self):
        """calculate standard delivery date for SPDM items. 
        """        
        # This one must run after CLOCK_START_DT is ready. 
        # And CLOCK_START_DT has to be a business day
        # other wise it can't be found in DURATION table.
        qs = {
            "std_del_dt" : f"""
                CREATE MULTISET VOLATILE TABLE STD_DEL AS (
                    SEL BASE.PIN
                        , DURATION.DDATE STD_DELIVER_DATE_DP0
					    , DURATION_DP1.DDATE STD_DELIVER_DATE_DP1 
                    FROM {self.item_master_tb} BASE
                    LEFT JOIN {projConst.SHIP_DURATION} DURATION
                        ON BASE.CLOCK_START_DT = DURATION.ODATE
                            AND BASE.OPROV = DURATION.OPROV
                            AND BASE.DPROV = DURATION.DPROV
                            AND BASE.STD = DURATION.DURATION
                    LEFT JOIN {projConst.SHIP_DURATION} DURATION_DP1
                        ON BASE.CLOCK_START_DT = DURATION_DP1.ODATE
                            AND BASE.OPROV = DURATION_DP1.OPROV
                            AND BASE.DPROV = DURATION_DP1.DPROV
                            AND (BASE.STD + 1) = DURATION_DP1.DURATION
                ) WITH DATA UNIQUE PRIMARY INDEX (PIN)
                ON COMMIT PRESERVE ROWS;
            """,
            
            'merge_into_master' : f"""
                MERGE INTO {self.item_master_tb} T1
                USING STD_DEL T2
                    ON T1.PIN = T2.PIN
                WHEN MATCHED THEN
                    UPDATE SET STD_DELIVER_DATE_DP0 = T2.STD_DELIVER_DATE_DP0
                        ,  STD_DELIVER_DATE_DP1 = T2.STD_DELIVER_DATE_DP1
            """, 
            
            'drop_temp_table' : 'DROP TABLE STD_DEL'
            
            }
        self.query_runner(qs)

    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def spe_induction(self):
        """collect induction scan related data for SPDM items
            and append the info to self.check_records_tb. 
        """        
        # induction doesn't check delay but set a start point for categorization step. 
        
        qs = {
            "spe_collect_induction" : f"""
                INSERT INTO {self.check_records_tb}
                    SEL ITEMS.PIN
                        , ITEMS.ITEM_ID
                        , {projConst.PAT_CAT_SEQ['INDUCTION']}
                        , NULL
                        , 'OT'
                        ,  17 CHECK_RESULT_BASIS_ID
                        , COALESCE(ITEMS.IND_WC, ITEMS.RECEIPT_SCAN_WC)
                        , COALESCE(ITEMS.IND_DTM, ITEMS.RECEIPT_SCAN_DTM_LOC)
                        , CASE 
                            WHEN ITEMS.IND_DTM IS NULL THEN ITEMS.RECEIPT_SCAN_DTM_UTC
                            ELSE NULL --when IND_DTM is availabe, it means it is from SPE source, which has no coresponding utc time. 
                        END
                        , ITEMS.IND_SRC
                        , 1 DP0_IND
                        , 1 DP1_IND
                    FROM {self.item_master_tb} ITEMS
            """
            }
        self.query_runner(qs)
        
    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def flash_induction(self):
        """collect induction scan related data for flash items a
            nd append the info to self.check_records_tb 
        """        
        # induction doesn't check delay but set a start point for categorization step. 
        qs = {
            "flash_collect_induction" : f"""
                INSERT INTO {self.check_records_tb}
                    SEL ITEMS.PIN
                        , ITEMS.ITEM_ID
                        , {projConst.PAT_CAT_SEQ['INDUCTION']}
                        , NULL
                        , 'OT'
                        ,  17 CHECK_RESULT_BASIS_ID
                        , COALESCE(ITEMS.MANIFEST_SCAN_WC, ITEMS.RECEIPT_SCAN_WC)
                        , COALESCE(ITEMS.MANIFEST_SCAN_DTM_LOC, ITEMS.RECEIPT_SCAN_DTM_LOC)
                        , COALESCE(ITEMS.MANIFEST_SCAN_DTM_UTC, ITEMS.RECEIPT_SCAN_DTM_UTC)
                        , COALESCE(ITEMS.MANIFEST_SCAN_CODE, ITEMS.RECEIPT_SCAN_CODE)
                        , 1 DP0_IND
                        , 1 DP1_IND
                    FROM {self.item_master_tb} ITEMS
            """
            }
        self.query_runner(qs)   
    
    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def check_special_case(self):
        """collect special case scans and append the info to self.check_records_tb
        """        
        qs = {
            "find_special_scans" : f"""
                INSERT INTO {self.check_records_tb}
                    SEL ITEMS.PIN
                        , ITEMS.ITEM_ID
                        , CASE SCANS.SPECIAL_EVENT_TYPE
                            WHEN 'CPC_MISSORT' THEN {projConst.PAT_CAT_SEQ['MISSORT_SCAN']}
                            WHEN 'FM' THEN {projConst.PAT_CAT_SEQ['FM']}
                            WHEN 'CUSTOMER_MISSORT' THEN {projConst.PAT_CAT_SEQ['CUSTOMER_MISSORT']}
                            WHEN 'UMO' THEN {projConst.PAT_CAT_SEQ['UMO']}
                        END PAT_CAT_SEQ 
                        , CAST(NULL AS TIMESTAMP(0)) CUTOFF_DTM
                        , 'LT' CHECK_RESULT_CODE
                        , CASE SCANS.SPECIAL_EVENT_TYPE
                            WHEN 'FM' THEN 8
                            WHEN 'CUSTOMER_MISSORT' THEN 9
                            WHEN 'CPC_MISSORT' THEN 28
                            WHEN 'UMO' THEN 29 -- no logical reason behind using 29 here
                        END CHECK_RESULT_BASIS_ID
                        , SCANS.SCAN_WC
                        , SCANS.SCAN_DTM_LOC
                        , SCANS.SCAN_DTM_UTC
                        , SCANS.SCAN_EVENT_CODE
                        , 1 DP0_IND
                        , 1 DP1_IND
                    FROM {self.item_master_tb} ITEMS
                    INNER JOIN {projConst.FCT_SPECIAL_EVENT_SCAN} SCANS
                        ON ITEMS.ITEM_ID = SCANS.ITEM_ID
                            AND SCANS.SPECIAL_EVENT_TYPE IN ('FM', 'CUSTOMER_MISSORT', 'CPC_MISSORT','UMO')
                            AND SCANS.ASC_SEQ = 1
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY ITEMS.{self.key_column}
                        ORDER BY SCANS.SCAN_DTM_UTC 
                    ) = 1   
            """,
            }
        self.query_runner(qs)
    
    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def originating_plt_first_ilc(self):
        
        """compare first originating plant processing scan against cut-off time. 
        """        
        # this shall be run after originating_plt_last_ilc 
        # so that it can get teh information if the first site
        # is the expected originating plant. 
        qs = {
            "check_oplt_first_ilc" : f"""
                INSERT INTO {self.check_records_tb}
                SEL ITEMS.PIN
                    , ITEMS.ITEM_ID
                    , {projConst.PAT_CAT_SEQ['OPLT_F_ILC']} PAT_CAT_SEQ
                    , CAST(BD.CUR_NEXT_BUS_DATE AS TIMESTAMP(0)) + 
                        CAST('{projConst.OPLT_F_PROC_CUTOFF_TIME}' AS INTERVAL HOUR TO SECOND)
                        AS OF_CUTOFF_DTM          -- alias has to be named differently from the name in SCAN (self.check_records_tb) table
                                                    -- otherwise in below statement it will use the same column name in SCAN table first. 
                    , CASE
                        WHEN ITEMS.BYPASS_WC IS NOT NULL 
                            OR ITEMS.DDI_FLG = 'T'
                            THEN 'SK'    --skip
                        --WHEN SCAN.CHECK_RESULT_BASIS_ID = 6 THEN 'UN' -- uncommented
                        WHEN ITEMS.F_PLT_F_PROC_DTM_LOC > OF_CUTOFF_DTM THEN 'LT'   --late
                        WHEN ITEMS.F_PLT_F_PROC_DTM_LOC <= OF_CUTOFF_DTM THEN 'OT'  --on-time

                        ELSE 'UN'       
                    END OF_CHECK_RESULT_CODE
                    
                    , CASE
                        WHEN ITEMS.BYPASS_WC IS NOT NULL THEN 1
                        WHEN ITEMS.DDI_FLG = 'T' THEN 2
                        WHEN SCAN.CHECK_RESULT_BASIS_ID = 6 THEN 6
                        WHEN OF_CHECK_RESULT_CODE <> 'UN' THEN 0
                        WHEN ITEMS.CLOCK_START_DT IS NULL THEN 4
                        WHEN ITEMS.F_PLT_F_PROC_DTM_LOC IS NULL THEN 5
                        WHEN BD.CUR_NEXT_BUS_DATE IS NULL THEN 7
                        ELSE 999
                    END CHECK_RESULT_BASIS_ID
                    
                    , ITEMS.F_PLT_F_PROC_WC
                    , ITEMS.F_PLT_F_PROC_DTM_LOC
                    , ITEMS.F_PLT_F_PROC_DTM_UTC
                    , 'PLANT_PROCESSING_SCAN' 
                    , 1 DP0_IND
                    , 1 DP1_IND
                FROM {self.item_master_tb} ITEMS
                LEFT JOIN {self.check_records_tb} SCAN 
                    ON ITEMS.{self.key_column} = SCAN.{self.key_column}
                        AND SCAN.PAT_CAT_SEQ = {projConst.PAT_CAT_SEQ['OPLT_L_ILC']}
                LEFT JOIN {projConst.BUSINESS_DAY} BD
                    ON ITEMS.CLOCK_START_DT + 1 = BD.CALENDAR_DATE
                        AND COALESCE(ITEMS.OPROV, 'ON') = BD.PROV_CODE 

            """, 
            }
        self.query_runner(qs)
        
    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def originating_plt_last_ilc(self):
        """compare last originating plant processing scan against cut-off time. 
        """        
        qs = {
            
            "get_ofsa_for_inbound" : f"""
                CREATE MULTISET VOLATILE TABLE INBOUND_OFSA AS (
                    SEL ITEMS.{self.key_column}
                        ,REGEXP_SUBSTR(SUBSTR(UPPER(TRIM(WC.site_postal_code)),1,3),'{projConst.FSA_REG_EXP}') OFSA
                    FROM {self.item_master_tb} ITEMS
                    INNER JOIN {SQConstants.SQ_WORKCENTRE_DIM} WC
                        ON ITEMS.RECEIPT_SCAN_WC = WC.WORK_CENTRE_ID
                            AND (ITEMS.SPE_DATA_TYPE = 'INBOUND'
                                OR ITEMS.RECEIPT_TYPE_CODE IN ('GE', 'NG', 'IB', 'IC')
                                )
                            AND ITEMS.OFSA IS NULL
                ) WITH DATA UNIQUE PRIMARY INDEX ({self.key_column})
                ON COMMIT PRESERVE ROWS;
            """,
            
            "merge_ofsa" : f"""
                MERGE INTO {self.item_master_tb} T1
                USING INBOUND_OFSA T2
                    ON T1.{self.key_column} = T2.{self.key_column}
                WHEN MATCHED THEN
                    UPDATE SET OFSA = T1.OFSA
            """,
          
            "get_all_originating_cut_offs" : f"""
                CREATE MULTISET VOLATILE TABLE ALL_OPLT_CUTOFF AS (
                    SEL ITEMS.PIN
                        , ITEMS.ITEM_ID
                        , {projConst.PAT_CAT_SEQ['OPLT_L_ILC']} PAT_CAT_SEQ
                        , ITEMS.CLOCK_START_DTM + CAST(LKP.DAY_AFTER AS INTERVAL DAY) 
                            +  (LKP.CUTOFF  - TIME '00:00:00' HOUR TO SECOND)
                            AS OL_CUTOFF_DTM
                            
                        , CASE
                            WHEN ITEMS.BYPASS_WC IS NOT NULL OR ITEMS.DDI_FLG = 'T' THEN 'SK'
                            WHEN ITEMS.F_PLT_L_PROC_DTM_LOC > OL_CUTOFF_DTM THEN 'LT'
                            WHEN ITEMS.F_PLT_L_PROC_DTM_LOC <= OL_CUTOFF_DTM THEN 'OT'
                            ELSE 'UN'
                        END OL_CHECK_RESULT_CODE
                        
                        , CASE
                            WHEN ITEMS.BYPASS_WC IS NOT NULL THEN 1
                            WHEN ITEMS.DDI_FLG = 'T' THEN 2
                            WHEN OL_CHECK_RESULT_CODE <> 'UN' THEN 0
                            WHEN ITEMS.CLOCK_START_DT IS NULL THEN 4
                            WHEN ITEMS.F_PLT_L_PROC_DTM_LOC IS NULL THEN 5
                            WHEN ITEMS.PRODUCT_CATEGORY IS NULL THEN 10
                            WHEN ITEMS.PRODUCT_CATEGORY IS NOT IN ('EP', 'XP', 'PC') THEN 11
                            WHEN ITEMS.OFSA IS NULL THEN 12
                            WHEN ITEMS.DFSA IS NULL THEN 13
                            WHEN ITEMS.LOGIC_F_PLT_PROC_SITE IS NOT NULL THEN 6
                            WHEN LKP.CUTOFF IS NULL THEN 14
                            ELSE 999
                        END OL_CHECK_RESULT_BASIS_ID
                        
                        , ITEMS.F_PLT_L_PROC_WC
                        , ITEMS.F_PLT_L_PROC_DTM_LOC
                        , ITEMS.F_PLT_L_PROC_DTM_UTC
                        , ROW_NUMBER() OVER (
                            PARTITION BY {self.key_column}
                            ORDER BY OL_CUTOFF_DTM DESC
                        ) OL_DUP_SEQ
                        
                    FROM {self.item_master_tb} ITEMS
                    LEFT JOIN {projConst.PAT_ORIGINATING_CUTOFF} LKP
                        ON ITEMS.OFSA BETWEEN LKP.OFSA_FROM AND LKP.OFSA_TO
                            AND ITEMS.DFSA BETWEEN LKP.DFSA_FROM AND LKP.DFSA_TO
                            AND ITEMS.F_PLT_L_PROC_WC = LKP.WORK_CENTRE_ID
                            AND ITEMS.DOW = LKP.DOW
                            AND ITEMS.PRODUCT_CATEGORY = LKP.PRODUCT
                ) WITH DATA UNIQUE PRIMARY INDEX ({self.key_column}, OL_DUP_SEQ)
                ON COMMIT PRESERVE ROWS
            """,
            
            "insert_unique_items_to_master" : f"""
                INSERT INTO {self.check_records_tb}
                    SEL PIN, ITEM_ID, PAT_CAT_SEQ, OL_CUTOFF_DTM, OL_CHECK_RESULT_CODE, OL_CHECK_RESULT_BASIS_ID
                        , F_PLT_L_PROC_WC, F_PLT_L_PROC_DTM_LOC, F_PLT_L_PROC_DTM_UTC
                        , 'PLANT_PROCESSING_SCAN'
                        , 1 DP0_IND
                        , 1 DP1_IND
                    FROM ALL_OPLT_CUTOFF
                    WHERE OL_DUP_SEQ = 1
            """,
            
            "get_dup_cutoff_item_id" : f"""
                CREATE MULTISET VOLATILE TABLE DUP_CO_ITEMS AS (
                    SEL {self.key_column}
                    FROM ALL_OPLT_CUTOFF
                    WHERE OL_DUP_SEQ = 2
                ) WITH DATA UNIQUE PRIMARY INDEX ({self.key_column})
                ON COMMIT PRESERVE ROWS;
            """,
            
            "collect_stats_after_oplt_l_ilc" : f"""
                COLLECT STATISTICS 
                    COLUMN (RELATED_SCAN_WC) , 
                    COLUMN ({self.key_column}),
                    COLUMN ({self.key_column} ,PAT_CAT_SEQ),
                    COLUMN (PAT_CAT_SEQ ,CHECK_RESULT_BASIS_ID),
                    COLUMN (PAT_CAT_SEQ) ,
                    COLUMN (CHECK_RESULT_BASIS_ID) ,
                    COLUMN (RELATED_SCAN_DTM_LOC)
                ON {self.check_records_tb};            
            """
            }
        self.query_runner(qs)
        
        if number_of_rows(self.cur, 'DUP_CO_ITEMS') > 0:
            
            # in case there are duplicated cutoff times
            # because of issues in orginating cutoff time table. 
            sql = f"""
                INSERT INTO {projConst.DUP_OPLT_CUTOFF}
                    SEL T1.*, 1, 1, CAST(CURRENT_TIMESTAMP(0) AS TIMESTAMP(0))
                    FROM ALL_OPLT_CUTOFF T1
                    WHERE EXISTS (
                        SEL 1 FROM DUP_CO_ITEMS T2 WHERE T1.{self.key_column} = T2.{self.key_column}
                        )
            """
            self.cur.execute(sql)
        
        self.drop_multiple_tables(['ALL_OPLT_CUTOFF','DUP_CO_ITEMS','INBOUND_OFSA'])
        
    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def dispatch_late(self, plant_type : str):
        """compare STV dispatch scan against cut-off time. 

        :param plant_type: 'o' originating plant, 'd' destination plant. 
        :type plant_type: str
        """        
        
        if plant_type == 'o':
            stv_cat_seq = projConst.PAT_CAT_SEQ['OPLT_DISPATCH']
            pre_ilc_seq = projConst.PAT_CAT_SEQ['OPLT_L_ILC']
        elif plant_type == 'd':
            stv_cat_seq = projConst.PAT_CAT_SEQ['DPLT_DISPATCH']
            pre_ilc_seq = projConst.PAT_CAT_SEQ['DPLT_L_ILC']
        
       
        qs = {
            
            'collect_check_record_stats' : f"COLLECT STATISTICS COLUMN ({self.key_column} ,DP0_IND) ON {self.check_records_tb}",
            
            'collect_item_for_stv_check' : f"""
                CREATE MULTISET VOLATILE TABLE ITEMS_FOR_STV AS (
                    SEL PIN, ITEM_ID, CUTOFF_DTM, RELATED_SCAN_WC, DP0_IND, DP1_IND
                    FROM {self.check_records_tb} ITEMS
                    WHERE ITEMS.PAT_CAT_SEQ = {pre_ilc_seq}
                        AND ITEMS.CHECK_RESULT_CODE = 'OT'
                ) WITH DATA UNIQUE PRIMARY INDEX ({self.key_column}, DP0_IND)
                ON COMMIT PRESERVE ROWS;
            """,
            
            'collect_item_for_stv_stats' :  f"""
                COLLECT STATISTICS 
                    COLUMN ({self.key_column}, DP0_IND)
                    , COLUMN (CUTOFF_DTM)
                    , COLUMN (RELATED_SCAN_WC)
                    , COLUMN ({self.key_column})
                ON ITEMS_FOR_STV
                """,
        }
        
        self.query_runner(qs)
        self.cur.execute('SEL MIN(CUTOFF_DTM), MAX(CUTOFF_DTM) FROM ITEMS_FOR_STV')
        dtms = self.cur.fetchone()
        min_dtm = dtms[0] 
        max_dtm = min(datetime.today(), dtms[1] + timedelta(days=projConst.SPE_SCAN_SEARCH_RANGE))
        
        qs = {
            "check_stv_against_cutoff" : f"""
                CREATE MULTISET VOLATILE TABLE STV_DEPARTURE AS (
                    SEL ITEMS.PIN
                        , ITEMS.ITEM_ID
                        , {stv_cat_seq} PAT_CAT_SEQ
                        , ITEMS.CUTOFF_DTM + INTERVAL '{projConst.STV_DEPARTURE_CUTOFF}' MINUTE STV_CUTOFF_DTM
                        , CASE 
                            WHEN SCANS.SCAN_DTM_LOCAL > STV_CUTOFF_DTM THEN 'LT'
                            ELSE 'OT'
                        END STV_CHECK_RESULT_CODE
                        , 0 STV_CHECK_RESULT_BASIS_ID
                        , SCANS.SCAN_WORK_CENTRE SCAN_WC
                        , CAST(CAST(SCANS.SCAN_DTM_LOCAL AS CHAR(19)) AS TIMESTAMP(0)) SCAN_DTM_LOC
                        , CAST(CAST(SCANS.SCAN_DTM_UTC AS CHAR(19)) AS TIMESTAMP(0)) SCAN_DTM_UTC
                        , 'STV_DISPATCH_SCAN' SCAN_TYPE
                        , ITEMS.DP0_IND
                        , ITEMS.DP1_IND
                    FROM ITEMS_FOR_STV ITEMS
                    INNER JOIN {SQConstants.EDW_SCAN_FCT} SCANS
                        ON ITEMS.ITEM_ID = SCANS.ITEM_ID
                            AND SCANS.SCAN_EVENT_CODE = '{projConst.STV_DEPARTURE_SCAN_CODE}'
                            AND SCANS.SCAN_DTM_LOCAL >= ITEMS.CUTOFF_DTM
                            AND SCANS.SCAN_DTM_LOCAL BETWEEN TIMESTAMP '{str(min_dtm)}'
                                AND TIMESTAMP '{str(max_dtm)}'
                    INNER JOIN {SQConstants.SQ_WORKCENTRE_DIM} ILC_WC
                        ON ITEMS.RELATED_SCAN_WC = ILC_WC.WORK_CENTRE_ID
                    INNER JOIN {SQConstants.SQ_WORKCENTRE_DIM} STV_WC
                        ON SCANS.SCAN_WORK_CENTRE = STV_WC.WORK_CENTRE_ID
                    WHERE ILC_WC.SITE_ID = STV_WC.SITE_ID
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY ITEMS.{self.key_column}, ITEMS.DP0_IND
                        ORDER BY SCANS.SCAN_DTM_UTC DESC --last STV scan
                    ) = 1
                ) WITH DATA UNIQUE PRIMARY INDEX ({self.key_column}, DP0_IND)
                ON COMMIT PRESERVE ROWS;
            """,
            
            "insert_into_master" : f"""
                INSERT INTO {self.check_records_tb}
                SEL T.* 
                FROM STV_DEPARTURE T
            """, 
            }
        self.query_runner(qs)
        self.drop_multiple_tables(['ITEMS_FOR_STV', 'STV_DEPARTURE'])
    
    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def destination_plt_first_ilc(self):
        """compare first originating plant processing scan against cut-off time. 
        """  
        qs = {
            "check_dplt_first_ilc" : f"""
                CREATE MULTISET VOLATILE TABLE ALL_DPLT_F_CUTOFF AS (
                    SEL ITEMS.PIN
                        , ITEMS.ITEM_ID
                        , CAST(ITEMS.STD_DELIVER_DATE_DP0 AS TIMESTAMP(0)) + 
                            CAST('{projConst.DPLT_F_PROC_CUTOFF_TIME}' AS INTERVAL HOUR TO SECOND)
                            AS DF_DP0_CUTOFF_DTM
                        , CAST(ITEMS.STD_DELIVER_DATE_DP1 AS TIMESTAMP(0)) + 
                            CAST('{projConst.DPLT_F_PROC_CUTOFF_TIME}' AS INTERVAL HOUR TO SECOND)
                            AS DF_DP1_CUTOFF_DTM
                            
                        , CASE
                            WHEN ITEMS.DDI_FLG = 'T' THEN 'SK' 
                            WHEN DSCAN.CHECK_RESULT_BASIS_ID = 6 THEN 'UN'
                            WHEN ITEMS.L_PLT_F_PROC_DTM_LOC > DF_DP0_CUTOFF_DTM THEN 'LT'                            
                            WHEN ITEMS.L_PLT_F_PROC_DTM_LOC <= DF_DP0_CUTOFF_DTM THEN 'OT'
                            ELSE 'UN'
                        END DF_CHECK_RESULT_CODE_DP0
                        
                        , CASE
                            WHEN DF_CHECK_RESULT_CODE_DP0 IN ('SK', 'UN') THEN DF_CHECK_RESULT_CODE_DP0
                            WHEN DSCAN.CHECK_RESULT_BASIS_ID = 6 THEN 'UN'
                            WHEN ITEMS.L_PLT_F_PROC_DTM_LOC > DF_DP1_CUTOFF_DTM THEN 'LT'
                            WHEN ITEMS.L_PLT_F_PROC_DTM_LOC <= DF_DP1_CUTOFF_DTM THEN 'OT'
                            ELSE 'UN'
                        END DF_CHECK_RESULT_CODE_DP1
                        
                        , CASE
                            WHEN ITEMS.DDI_FLG = 'T' THEN 2
                            WHEN DSCAN.CHECK_RESULT_BASIS_ID = 6 THEN 6
                            WHEN DF_CHECK_RESULT_CODE_DP0 <> 'UN' THEN 0
                            WHEN ITEMS.STD_DELIVER_DATE_DP0 IS NULL THEN 15
                            WHEN ITEMS.L_PLT_F_PROC_DTM_LOC IS NULL THEN 5
                            ELSE 999
                        END DF_CHECK_RESULT_BASIS_ID
                        
                        , ITEMS.L_PLT_F_PROC_WC SCAN_WC
                        , ITEMS.L_PLT_F_PROC_DTM_LOC SCAN_DTM_LOC
                        , ITEMS.L_PLT_F_PROC_DTM_UTC SCAN_DTM_UTC
                    FROM {self.item_master_tb} ITEMS
                     
                    -- use DSCAN items to get items which considered have destination plt scan. 
                    INNER JOIN {self.check_records_tb} DSCAN
                        ON ITEMS.{self.key_column} = DSCAN.{self.key_column}
                            AND DSCAN.PAT_CAT_SEQ = {projConst.PAT_CAT_SEQ['DPLT_L_ILC']}
                            AND DSCAN.DP0_IND = 1                    
                ) WITH DATA UNIQUE PRIMARY INDEX ({self.key_column})
                ON COMMIT PRESERVE ROWS
            """, 
            
            # "dump_vtb" : '''
            #     CREATE MULTISET TABLE DL_SQ_PRD_WRK.A28_ALL_DPLT_F_CUTOFF AS (
            #         SEL * FROM ALL_DPLT_F_CUTOFF
            #     ) WITH DATA PRIMARY INDEX (ITEM_ID)
            # ''', 
            
            "insert_dp0_to_master" : f"""
                INSERT INTO {self.check_records_tb}
                    SEL PIN, ITEM_ID, {projConst.PAT_CAT_SEQ['DPLT_F_ILC']}, DF_DP0_CUTOFF_DTM
                        , DF_CHECK_RESULT_CODE_DP0, DF_CHECK_RESULT_BASIS_ID
                        , SCAN_WC, SCAN_DTM_LOC, SCAN_DTM_UTC, 'PLANT_PROCESSING_SCAN'
                        , 1 DP0_IND, 0 DP1_IND
                    FROM ALL_DPLT_F_CUTOFF
            """,
            
            "insert_dp1_to_master" : f"""
                INSERT INTO {self.check_records_tb}
                    SEL PIN, ITEM_ID, {projConst.PAT_CAT_SEQ['DPLT_F_ILC']}, DF_DP1_CUTOFF_DTM
                        , DF_CHECK_RESULT_CODE_DP1, DF_CHECK_RESULT_BASIS_ID
                        , SCAN_WC, SCAN_DTM_LOC, SCAN_DTM_UTC, 'PLANT_PROCESSING_SCAN'
                        , 0 DP0_IND, 1 DP1_IND
                    FROM ALL_DPLT_F_CUTOFF
            """,
            
            "drop_temp_table" :  'DROP TABLE ALL_DPLT_F_CUTOFF'
            
            }
        self.query_runner(qs)
        
    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def destination_plt_last_ilc(self):
        """compare last originating plant processing scan against cut-off time. 
        """  
        qs = {
            
            "outbound_cutoff" : f"""
                CREATE MULTISET VOLATILE TABLE OUTBOUND_CUTOFF AS (
                    SEL ITEMS.PIN
                        , ITEMS.ITEM_ID
                        , CAST(ITEMS.STD_DELIVER_DATE_DP0 AS TIMESTAMP(0)) 
                            + (OTBD.CUTOFF - TIME '00:00:00' HOUR TO SECOND) AS DL_DP0_CUTOFF_DTM
                            
                        , CAST(ITEMS.STD_DELIVER_DATE_DP1 AS TIMESTAMP(0)) 
                            + (OTBD.CUTOFF - TIME '00:00:00' HOUR TO SECOND) AS DL_DP1_CUTOFF_DTM
                            
                        , CASE
                            WHEN ITEMS.L_PLT_L_PROC_DTM_LOC > DL_DP0_CUTOFF_DTM THEN 'LT'
                            WHEN ITEMS.L_PLT_L_PROC_DTM_LOC <= DL_DP0_CUTOFF_DTM THEN 'OT'
                            ELSE 'UN'
                        END DL_CHECK_RESULT_CODE_DP0
                        
                        , CASE
                            WHEN DL_CHECK_RESULT_CODE_DP0 IN ('SK', 'UN') THEN DL_CHECK_RESULT_CODE_DP0
                            WHEN ITEMS.L_PLT_L_PROC_DTM_LOC > DL_DP1_CUTOFF_DTM THEN 'LT'
                            WHEN ITEMS.L_PLT_L_PROC_DTM_LOC <= DL_DP1_CUTOFF_DTM THEN 'OT'
                            ELSE 'UN'
                        END DL_CHECK_RESULT_CODE_DP1
                        
                        , CASE
                            WHEN DL_CHECK_RESULT_CODE_DP0 <> 'UN' THEN 0
                            WHEN ITEMS.STD_DELIVER_DATE_DP0 IS NULL THEN 15
                            WHEN ITEMS.L_PLT_L_PROC_DTM_LOC IS NULL THEN 5
                            WHEN ITEMS.PRODUCT IS NULL THEN 21
                            WHEN ITEMS.PRODUCT NOT IN ({projConst.OUTBOUND_PRODUCT_TYPES}) THEN 22
                            WHEN ITEMS.L_PLT_F_PROC_SITE NOT IN ({projConst.OUTBOUND_EXCHANGE_OFFICE_SITE_ID}) THEN 23
                            WHEN ITEMS.OUTBOUND_US_ZIP NOT BETWEEN '000' AND '999' THEN 24
                            WHEN DL_DP0_CUTOFF_DTM IS NULL THEN 25
                            ELSE 999
                        END CHECK_RESULT_BASIS_ID
                        
                        , ITEMS.L_PLT_L_PROC_WC SCAN_WC
                        , ITEMS.L_PLT_L_PROC_DTM_LOC SCAN_DTM_LOC
                        , ITEMS.L_PLT_L_PROC_DTM_UTC SCAN_DTM_UTC
                        
                        , CASE
                            WHEN COALESCE(ITEMS.LOGIC_F_PLT_PROC_SITE, ITEMS.F_PLT_F_PROC_SITE) = ITEMS.L_PLT_F_PROC_SITE THEN 'T'
                            ELSE 'F'
                        END OB_EX_IS_ORIGIN_PLANT
                            
                    FROM {self.item_master_tb} ITEMS
                    LEFT JOIN {projConst.PAT_OUTBOUND_CUTOFF} OTBD
                        ON ITEMS.L_PLT_F_PROC_SITE = OTBD.SITE_ID
                            AND ITEMS.PRODUCT = OTBD.PRODUCT
                            AND TD_DAY_OF_WEEK(ITEMS.STD_DELIVER_DATE_DP1) = OTBD.DOW 
                    WHERE ITEMS.OUTBOUND_FLG = 'T'
                ) WITH DATA UNIQUE PRIMARY INDEX ({self.key_column})
                ON COMMIT PRESERVE ROWS;
            """,            
            
            "remove_oplant_last_scan_check_result_when_exo_is_then_only_plant" : f"""
                DEL {self.check_records_tb}
                FROM OUTBOUND_CUTOFF T1
                WHERE {self.check_records_tb}.ITEM_ID = T1.ITEM_ID
                    AND T1.OB_EX_IS_ORIGIN_PLANT = 'T'
                    AND {self.check_records_tb}.PAT_CAT_SEQ = {projConst.PAT_CAT_SEQ['OPLT_L_ILC']}
            """,
            
            "insert_outbound_last_plant_cutoff_dp0" : f"""
                INSERT INTO {self.check_records_tb}
                SEL PIN, ITEM_ID, {projConst.PAT_CAT_SEQ['DPLT_L_ILC']}
                    , DL_DP0_CUTOFF_DTM, DL_CHECK_RESULT_CODE_DP0
                    , CHECK_RESULT_BASIS_ID, SCAN_WC, SCAN_DTM_LOC
                    , SCAN_DTM_UTC, 'PLANT_PROCESSING_SCAN'
                    , 1 DP0_IND, 0 DP1_IND     
                FROM OUTBOUND_CUTOFF T1
            """,
            
            "insert_outbound_last_plant_cutoff_dp1" : f"""
                INSERT INTO {self.check_records_tb}
                SEL PIN, ITEM_ID, {projConst.PAT_CAT_SEQ['DPLT_L_ILC']}
                    , DL_DP1_CUTOFF_DTM, DL_CHECK_RESULT_CODE_DP1
                    , CHECK_RESULT_BASIS_ID, SCAN_WC, SCAN_DTM_LOC
                    , SCAN_DTM_UTC, 'PLANT_PROCESSING_SCAN'
                    , 0 DP0_IND, 1 DP1_IND     
                FROM OUTBOUND_CUTOFF T1
            """,
         
            "dp0_cutoff" : f"""
                CREATE MULTISET VOLATILE TABLE DPLT_CUTOFF AS (
                    SEL 
                        ITEMS.PIN
                        ,ITEMS.ITEM_ID
                        
                        , COALESCE(DFSA.PRE_DAY, DCNTR.PRE_DAY, DWC.PRE_DAY) DL_CUTOFF_PREDAY
                        , COALESCE(DFSA.CUTOFF, DCNTR.CUTOFF, DWC.CUTOFF) DL_CUTOFF_TIME
                        
                        , CAST(ITEMS.STD_DELIVER_DATE_DP0 - DL_CUTOFF_PREDAY AS TIMESTAMP(0)) 
                            + (DL_CUTOFF_TIME  - TIME '00:00:00' HOUR TO SECOND) AS DL_DP0_CUTOFF_DTM
                            
                        , CAST(ITEMS.STD_DELIVER_DATE_DP1 - DL_CUTOFF_PREDAY AS TIMESTAMP(0)) 
                            + (DL_CUTOFF_TIME  - TIME '00:00:00' HOUR TO SECOND) AS DL_DP1_CUTOFF_DTM
                            
                        , CASE
                            WHEN ITEMS.DDI_FLG = 'T' THEN 'SK'
                            WHEN ITEMS.L_PLT_L_PROC_DTM_LOC > DL_DP0_CUTOFF_DTM THEN 'LT'
                            WHEN ITEMS.L_PLT_L_PROC_DTM_LOC <= DL_DP0_CUTOFF_DTM THEN 'OT'
                            ELSE 'UN'
                        END DL_CHECK_RESULT_CODE_DP0
                        
                        , CASE
                            WHEN DL_CHECK_RESULT_CODE_DP0 IN ('SK', 'UN') THEN DL_CHECK_RESULT_CODE_DP0
                            WHEN ITEMS.L_PLT_L_PROC_DTM_LOC > DL_DP1_CUTOFF_DTM THEN 'LT'
                            WHEN ITEMS.L_PLT_L_PROC_DTM_LOC <= DL_DP1_CUTOFF_DTM THEN 'OT'
                            ELSE 'UN'
                        END DL_CHECK_RESULT_CODE_DP1
                        
                        , CASE
                            WHEN ITEMS.DDI_FLG = 'T' THEN 2
                            WHEN DL_CHECK_RESULT_CODE_DP0 <> 'UN' THEN 0
                            WHEN ITEMS.STD_DELIVER_DATE_DP0 IS NULL THEN 15 --if dp0 null then dp1 will be null as well, vice versa. 
                            WHEN ITEMS.L_PLT_L_PROC_DTM_LOC IS NULL THEN 5
                            WHEN ITEMS.PRODUCT_CATEGORY IS NULL THEN 10
                            WHEN ITEMS.PRODUCT_CATEGORY IS NOT IN ('EP', 'XP', 'PC') THEN 11
                            WHEN ITEMS.LOGIC_L_PLT_PROC_SITE IS NOT NULL THEN 6
                            WHEN DL_CUTOFF_TIME IS NULL THEN 14
                            ELSE 999
                        END DL_CHECK_RESULT_BASIS_ID
                        
                        , ITEMS.L_PLT_L_PROC_WC SCAN_WC
                        , ITEMS.L_PLT_L_PROC_DTM_LOC SCAN_DTM_LOC
                        , ITEMS.L_PLT_L_PROC_DTM_UTC SCAN_DTM_UTC
                        
                        , ROW_NUMBER() OVER (
                            PARTITION BY ITEMS.{self.key_column}
                            ORDER BY DL_DP0_CUTOFF_DTM DESC
                        ) DUP_SEQ
                        
                    FROM {self.item_master_tb} ITEMS
                    
                    LEFT JOIN {projConst.PAT_DEST_CUTOFF_WC} DWC
                        ON ITEMS.L_PLT_L_PROC_WC = DWC.WORK_CENTRE_ID
                            AND ITEMS.PRODUCT_CATEGORY = DWC.PRODUCT
                    
                    LEFT JOIN {projConst.PAT_DEST_CUTOFF_CNTR} DCNTR
                        ON ITEMS.L_PLT_L_PROC_WC = DCNTR.WORK_CENTRE_ID
                            AND ITEMS.DCNTR = DCNTR.DCNTR
                            AND ITEMS.PRODUCT_CATEGORY = DCNTR.PRODUCT
                            
                    LEFT JOIN {projConst.PAT_DEST_CUTOFF_FSA} DFSA
                        ON ITEMS.L_PLT_L_PROC_WC = DFSA.WORK_CENTRE_ID
                            AND ITEMS.DFSA = DFSA.DFSA
                            AND ITEMS.PRODUCT_CATEGORY = DFSA.PRODUCT
                            

                    LEFT JOIN {self.check_records_tb} OSCAN  
                        ON ITEMS.{self.key_column} = OSCAN.{self.key_column}
                            AND OSCAN.PAT_CAT_SEQ = {projConst.PAT_CAT_SEQ['OPLT_F_ILC']}
                            
                    
                    
                    WHERE  --conditions to get items which have destination plant
                        
                        -- bypass one always has destination plant
                        (ITEMS.BYPASS_WC IS NOT NULL
                        
                        -- originating plant is identified, then if last ilc site
                        -- <> first ilc site, last ilc is the destination plant scan
                        OR (OSCAN.CHECK_RESULT_CODE <> 'UN'
                            AND ITEMS.F_PLT_F_PROC_SITE <> ITEMS.L_PLT_F_PROC_SITE
                        )
                        
                        -- first ilc is not expected originating plant. 
                        -- then last ilc (even it is same as first ilc)                        
                        -- should be destinaton plant ilc. 
                       -- OR OSCAN.CHECK_RESULT_BASIS_ID = 6 THIS IS REMOVED FOR TESTING
                        
                        OR ITEMS.F_PLT_F_PROC_SITE <> ITEMS.L_PLT_F_PROC_SITE
                        -- COALESCE(ITEMS.LOGIC_F_PLT_PROC_SITE, ITEMS.F_PLT_F_PROC_SITE) <> ITEMS.L_PLT_F_PROC_SITE
                          -- note:Changing this logic to exclude the 500 and 600 records from the scans table if the origin and destination plant is same.
                        ) AND ITEMS.OUTBOUND_FLG = 'F'
                            
                ) WITH DATA UNIQUE PRIMARY INDEX ({self.key_column}, DUP_SEQ)
                ON COMMIT PRESERVE ROWS
            """,

            "insert_dp0_to_master" : f"""
                INSERT INTO {self.check_records_tb}
                    SEL PIN, ITEM_ID, {projConst.PAT_CAT_SEQ['DPLT_L_ILC']}, DL_DP0_CUTOFF_DTM, 
                        DL_CHECK_RESULT_CODE_DP0, DL_CHECK_RESULT_BASIS_ID
                        , SCAN_WC, SCAN_DTM_LOC, SCAN_DTM_UTC, 'PLANT_PROCESSING_SCAN'
                        , 1 DP0_IND, 0 DP1_IND     
                    FROM DPLT_CUTOFF
                    WHERE DUP_SEQ = 1
            """,
            
            "insert_dp1_to_master" : f"""
                INSERT INTO {self.check_records_tb}
                    SEL PIN, ITEM_ID, {projConst.PAT_CAT_SEQ['DPLT_L_ILC']}, DL_DP1_CUTOFF_DTM
                        , DL_CHECK_RESULT_CODE_DP1, DL_CHECK_RESULT_BASIS_ID
                        , SCAN_WC, SCAN_DTM_LOC, SCAN_DTM_UTC, 'PLANT_PROCESSING_SCAN'
                        , 0 DP0_IND, 1 DP1_IND
                    FROM DPLT_CUTOFF
                    WHERE DUP_SEQ = 1
            """,
            
            "get_dup_cutoff_item_id" : f"""
                CREATE MULTISET VOLATILE TABLE DUP_CO_ITEMS AS (
                    SEL {self.key_column}
                    FROM DPLT_CUTOFF
                    WHERE DUP_SEQ = 2
                ) WITH DATA UNIQUE PRIMARY INDEX ({self.key_column})
                ON COMMIT PRESERVE ROWS;
            """,
             
            
            "collect_stats_after_dplt_l_ilc" : f"""
                COLLECT STATISTICS 
                    COLUMN (RELATED_SCAN_WC) ,
                    COLUMN ({self.key_column} ,PAT_CAT_SEQ),
                    COLUMN ({self.key_column}),
                    COLUMN (PAT_CAT_SEQ ,CHECK_RESULT_BASIS_ID),
                    COLUMN (PAT_CAT_SEQ) ,
                    COLUMN (CHECK_RESULT_BASIS_ID) ,
                    COLUMN (RELATED_SCAN_DTM_LOC)
                ON {self.check_records_tb};            
            """
            }
        self.query_runner(qs)
        
        if number_of_rows(self.cur, 'DUP_CO_ITEMS') > 0: 
            sql = f"""
                INSERT INTO {projConst.DUP_DPLT_CUTOFF}
                    SEL PIN, ITEM_ID, {projConst.PAT_CAT_SEQ['DPLT_L_ILC']}, DL_DP1_CUTOFF_DTM
                        , DL_CHECK_RESULT_CODE_DP1, DL_CHECK_RESULT_BASIS_ID
                        , SCAN_WC, SCAN_DTM_LOC, SCAN_DTM_UTC, DUP_SEQ
                        , 0 DP0_IND, 1 DP1_IND, CAST(CURRENT_TIMESTAMP(0) AS TIMESTAMP(0))
                    FROM DPLT_CUTOFF T1
                    WHERE EXISTS (
                        SEL 1 FROM DUP_CO_ITEMS T2 WHERE T1.{self.key_column} = T2.{self.key_column}
                        )
                        
                    UNION
                    
                    SEL PIN, T3.ITEM_ID, {projConst.PAT_CAT_SEQ['DPLT_L_ILC']}, DL_DP0_CUTOFF_DTM
                        , DL_CHECK_RESULT_CODE_DP0, DL_CHECK_RESULT_BASIS_ID
                        , SCAN_WC, SCAN_DTM_LOC, SCAN_DTM_UTC, DUP_SEQ
                        , 1 DP0_IND, 0 DP1_IND, CAST(CURRENT_TIMESTAMP(0) AS TIMESTAMP(0))
                    FROM DPLT_CUTOFF T3
                    WHERE EXISTS (
                        SEL 1 FROM DUP_CO_ITEMS T4 WHERE T3.{self.key_column} = T4.{self.key_column}
                        )
            """
            self.cur.execute(sql) # the above query is not excuted because we need to fix this as table overflows due to duplicate data.
            
            
        self.drop_multiple_tables(['DPLT_CUTOFF', 'DUP_CO_ITEMS','OUTBOUND_CUTOFF'])
    
    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def dss(self):
        
        qs = {
            "leg_1_end" : f"""
                CREATE MULTISET VOLATILE TABLE LEG1_ITEM AS (
                    SEL ITEMS.PIN
                        , ITEMS.ITEM_ID
                        , COALESCE(LKP_ZIP.CUTOFF, LKP_SITE.CUTOFF, CAST(NULL AS TIME)) OUTBOUND_CUTOFFTM
                        
                        , CAST(ITEMS.STD_DELIVER_DATE_DP0 AS TIMESTAMP(0)) + 
                            ((OUTBOUND_CUTOFFTM - TIME '00:00:00') HOUR TO SECOND)
                            AS DP0_CUTOFF_DTM
                        , CAST(ITEMS.STD_DELIVER_DATE_DP1 AS TIMESTAMP(0)) + 
                            ((OUTBOUND_CUTOFFTM - TIME '00:00:00') HOUR TO SECOND)
                            AS DP1_CUTOFF_DTM
                        
                        , CASE
                            WHEN ITEMS.L_DEPOT_F_DSS_DTM_LOC > DP0_CUTOFF_DTM THEN 'LT'
                            WHEN ITEMS.L_DEPOT_F_DSS_DTM_LOC <= DP0_CUTOFF_DTM THEN 'OT'
                            ELSE 'UN'
                        END CHECK_RESULT_CODE_DP0
                        
                        , CASE
                            WHEN ITEMS.L_DEPOT_F_DSS_DTM_LOC > DP1_CUTOFF_DTM THEN 'LT'
                            WHEN ITEMS.L_DEPOT_F_DSS_DTM_LOC <= DP1_CUTOFF_DTM THEN 'OT'
                            ELSE 'UN'
                        END CHECK_RESULT_CODE_DP1
                        
                        , CASE
                            WHEN CHECK_RESULT_CODE_DP0 <> 'UN' THEN 0
                            WHEN ITEMS.STD_DELIVER_DATE_DP0 IS NULL THEN 15
                            WHEN ITEMS.L_DEPOT_F_DSS_DTM_LOC IS NULL THEN 16
                            WHEN ITEMS.PRODUCT IS NULL THEN 21
                            WHEN ITEMS.PRODUCT NOT IN ({projConst.OUTBOUND_PRODUCT_TYPES}) THEN 22
                            WHEN WC.SITE_ID NOT IN ({projConst.OUTBOUND_EXCHANGE_OFFICE_SITE_ID}) THEN 23
                            WHEN ITEMS.OUTBOUND_US_ZIP NOT BETWEEN '000' AND '999' THEN 24
                            WHEN OUTBOUND_CUTOFFTM IS NULL THEN 25
                            ELSE 999
                        END CHECK_RESULT_BASIS_ID
                        
                        , ITEMS.L_DEPOT_F_DSS_WC SCAN_WC
                        , ITEMS.L_DEPOT_F_DSS_DTM_LOC SCAN_DTM_LOC
                        , ITEMS.L_DEPOT_F_DSS_DTM_UTC SCAN_DTM_UTC   
                            
                    FROM {self.item_master_tb} ITEMS
                    INNER JOIN {projConst.LKP_KEY_SCAN} LKP
                        ON ITEMS.L_DEPOT_F_DSS_SCAN_CODE = LKP.SCAN_EVENT_CODE
                            AND LKP.SCAN_TYPE_L1 = 'LEG_1_END'
                    INNER JOIN {SQConstants.SQ_WORKCENTRE_DIM} WC
                        ON ITEMS.L_DEPOT_F_DSS_WC = WC.WORK_CENTRE_ID
                    LEFT JOIN {projConst.LEG1_END_CUTOFF} LKP_SITE
                        ON ITEMS.PRODUCT = LKP_SITE.PRODUCT
                            AND WC.SITE_ID = LKP_SITE.SITE_ID
                            AND LKP_SITE.ZIP_CODE_START IS NULL
                    LEFT JOIN {projConst.LEG1_END_CUTOFF} LKP_ZIP
                        ON ITEMS.PRODUCT = LKP_SITE.PRODUCT
                            AND WC.SITE_ID = LKP_SITE.SITE_ID
                            AND ITEMS.OUTBOUND_US_ZIP BETWEEN
                                LKP_SITE.ZIP_CODE_START AND LKP_SITE.ZIP_CODE_START
                ) WITH DATA UNIQUE PRIMARY INDEX ({self.key_column})
                ON COMMIT PRESERVE ROWS;
            """,
                        
            "insert_leg1_dp0_to_master" : f"""
                INSERT INTO {self.check_records_tb}
                    SEL PIN, ITEM_ID, {projConst.PAT_CAT_SEQ['DSS']}, DP0_CUTOFF_DTM
                        , CHECK_RESULT_CODE_DP0, CHECK_RESULT_BASIS_ID
                        , SCAN_WC, SCAN_DTM_LOC, SCAN_DTM_UTC, 'DSS_SCAN'
                        , 1 DP0_IND, 0 DP1_IND
                    FROM LEG1_ITEM
            """,
            
            "insert_leg1_dp1_to_master" : f"""
                INSERT INTO {self.check_records_tb}
                    SEL PIN, ITEM_ID, {projConst.PAT_CAT_SEQ['DSS']}, DP1_CUTOFF_DTM
                        , CHECK_RESULT_CODE_DP1, CHECK_RESULT_BASIS_ID
                        , SCAN_WC, SCAN_DTM_LOC, SCAN_DTM_UTC, 'DSS_SCAN'
                        , 0 DP0_IND, 1 DP1_IND
                    FROM LEG1_ITEM
            """,
            
            "dss_cutoff" : f"""
                CREATE MULTISET VOLATILE TABLE DSS_CUTOFF AS (
                    SEL ITEMS.PIN
                        , ITEMS.ITEM_ID
                        
                        , CAST(ITEMS.STD_DELIVER_DATE_DP0 AS TIMESTAMP(0)) + 
                            CAST('{projConst.DEPOT_CUTOFF_TIME}' AS INTERVAL HOUR TO SECOND)
                            AS DP0_CUTOFF_DTM
                        , CAST(ITEMS.STD_DELIVER_DATE_DP1 AS TIMESTAMP(0)) + 
                            CAST('{projConst.DEPOT_CUTOFF_TIME}' AS INTERVAL HOUR TO SECOND)
                            AS DP1_CUTOFF_DTM
                            
                        , CASE
                            WHEN ITEMS.L_DEPOT_F_DSS_DTM_LOC > DP0_CUTOFF_DTM THEN 'LT'
                            WHEN ITEMS.L_DEPOT_F_DSS_DTM_LOC <= DP0_CUTOFF_DTM THEN 'OT'
                            ELSE 'UN'
                        END CHECK_RESULT_CODE_DP0
                        
                        , CASE
                            WHEN ITEMS.L_DEPOT_F_DSS_DTM_LOC > DP1_CUTOFF_DTM THEN 'LT'
                            WHEN ITEMS.L_DEPOT_F_DSS_DTM_LOC <= DP1_CUTOFF_DTM THEN 'OT'
                            ELSE 'UN'
                        END CHECK_RESULT_CODE_DP1
                        
                        , CASE
                            WHEN CHECK_RESULT_CODE_DP0 <> 'UN' THEN 0
                            WHEN SPE_DATA_TYPE = 'OUTBOUND'
                                OR OUTBOUND_FLG = 'T' THEN 20
                            WHEN ITEMS.STD_DELIVER_DATE_DP0 IS NULL THEN 15
                            WHEN ITEMS.L_DEPOT_F_DSS_DTM_LOC IS NULL THEN 16
                            ELSE 999
                        END CHECK_RESULT_BASIS_ID
                        
                        , ITEMS.L_DEPOT_F_DSS_WC SCAN_WC
                        , ITEMS.L_DEPOT_F_DSS_DTM_LOC SCAN_DTM_LOC
                        , ITEMS.L_DEPOT_F_DSS_DTM_UTC SCAN_DTM_UTC
                    FROM {self.item_master_tb} ITEMS
                    WHERE NOT EXISTS (
                        SEL 1 FROM LEG1_ITEM 
                        WHERE ITEMS.{self.key_column} = LEG1_ITEM.{self.key_column}
                    )
                ) WITH DATA UNIQUE PRIMARY INDEX ({self.key_column})
                ON COMMIT PRESERVE ROWS
            """, 
            
            "insert_dss_dp0_to_master" : f"""
                INSERT INTO {self.check_records_tb}
                    SEL PIN, ITEM_ID, {projConst.PAT_CAT_SEQ['DSS']}, DP0_CUTOFF_DTM
                        , CHECK_RESULT_CODE_DP0, CHECK_RESULT_BASIS_ID
                        , SCAN_WC, SCAN_DTM_LOC, SCAN_DTM_UTC, 'DSS_SCAN'
                        , 1 DP0_IND, 0 DP1_IND
                    FROM DSS_CUTOFF
            """,
            
            "insert_dss_dp1_to_master" : f"""
                INSERT INTO {self.check_records_tb}
                    SEL PIN, ITEM_ID, {projConst.PAT_CAT_SEQ['DSS']}, DP1_CUTOFF_DTM
                        , CHECK_RESULT_CODE_DP1, CHECK_RESULT_BASIS_ID
                        , SCAN_WC, SCAN_DTM_LOC, SCAN_DTM_UTC, 'DSS_SCAN'
                        , 0 DP0_IND, 1 DP1_IND
                    FROM DSS_CUTOFF
            """
            }
        self.query_runner(qs)
        
        self.drop_multiple_tables(['LEG1_ITEM', 'DSS_CUTOFF'])
        
    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def dss_ontime_del_late(self):
        qs = {
            "insert_dss_ontime_del_late_dp0" : f"""
                INSERT INTO {self.check_records_tb}
                    SEL SCANS.PIN
                        , SCANS.ITEM_ID
                        , {projConst.PAT_CAT_SEQ['DSS_OT_DL_LATE']}
                        , CAST(ITEMS.STD_DELIVER_DATE_DP0 AS TIMESTAMP(0)) +
                            CAST('{projConst.DELIVERY_CUTOFF_TIME}' AS INTERVAL HOUR TO SECOND)
                        , 'LT'
                        , 0
                        , F_ATT_DEL_WC
                        , F_ATT_DEL_DTM_LOC
                        , F_ATT_DEL_DTM_UTC
                        , F_ATT_DEL_SCAN_CODE
                        , 1
                        , 0
                    FROM {self.check_records_tb} SCANS
                    INNER JOIN {self.item_master_tb} ITEMS
                        ON SCANS.{self.key_column} = ITEMS.{self.key_column}
                            AND SCANS.PAT_CAT_SEQ = {projConst.PAT_CAT_SEQ['DSS']}
                            AND SCANS.CHECK_RESULT_CODE = 'OT'
                            AND ITEMS.DP0 = 0 --DP0 fail
                            AND SCANS.DP0_IND = 1
            """,
            
            "insert_dss_ontime_del_late_dp1" : f"""
                INSERT INTO {self.check_records_tb}
                    SEL SCANS.PIN
                        , SCANS.ITEM_ID
                        , {projConst.PAT_CAT_SEQ['DSS_OT_DL_LATE']}
                        , CAST(ITEMS.STD_DELIVER_DATE_DP1 AS TIMESTAMP(0)) +
                            CAST('{projConst.DELIVERY_CUTOFF_TIME}' AS INTERVAL HOUR TO SECOND)
                        , 'LT'
                        , 0
                        , F_ATT_DEL_WC
                        , F_ATT_DEL_DTM_LOC
                        , F_ATT_DEL_DTM_UTC
                        , F_ATT_DEL_SCAN_CODE
                        , 0
                        , 1
                    FROM {self.check_records_tb} SCANS
                    INNER JOIN {self.item_master_tb} ITEMS
                        ON SCANS.{self.key_column} = ITEMS.{self.key_column}
                            AND SCANS.PAT_CAT_SEQ = {projConst.PAT_CAT_SEQ['DSS']}
                            AND SCANS.CHECK_RESULT_CODE = 'OT'
                            AND ITEMS.DP1 = 0 --DP1 fail
                            AND SCANS.DP1_IND = 1
            """,
            }
        self.query_runner(qs)
        
    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    
    def missort(self):
        
        qs = {
            "###collect_all_ilc_dss_scans+ALL_SCANS" : f"""
                CREATE MULTISET VOLATILE TABLE ALL_SCANS AS 
                (
                SEL ITEMS.ITEM_ID, ITEMS.PIN, CAST(CAST(SCAN.SCAN_DTM_UTC AS CHAR(19)) AS TIMESTAMP(0)) SCAN_DTM_UTC, 
                CAST(CAST(SCAN.SCAN_DTM_LOCAL AS CHAR(19)) AS TIMESTAMP(0)) SCAN_DTM_LOCAL, 
                SCAN.SCAN_WORK_CENTRE, SCAN.SCAN_EVENT_CODE, WC.SITE_ID, ROW_NUMBER() OVER (PARTITION BY ITEMS.PIN 
                ORDER BY
                SCAN.SCAN_DTM_UTC )  ID
                FROM
                {self.item_master_tb} ITEMS 
                INNER JOIN PARCEL_SEM_VW.ITEM_SCAN_EVENT_FCT SCAN 
                ON ITEMS.{self.key_column} = SCAN.{self.edw_key_column}
                INNER JOIN DL_SQ_PRD_INT.LKP_KEY_SCAN LKP 
                ON SCAN.SCAN_EVENT_CODE = LKP.SCAN_EVENT_CODE 
                --AND LKP.SCAN_TYPE_L1 IN ('DEPOT_PROC', 'PLANT_PROC')
                INNER JOIN DL_SQ_PRD_INT.DIM_WORK_CENTRE_ORG WC 
                ON SCAN.SCAN_WORK_CENTRE = WC.WORK_CENTRE_ID
                WHERE LKP.SCAN_TYPE_L1 IN ('DEPOT_PROC', 'PLANT_PROC','CPC_MISSORT')
                -- This is new addition to filter out PINS with RTS scans from Missort
                AND ITEMS.PIN NOT IN (
                SELECT DISTINCT ITEMS.PIN
                FROM {self.item_master_tb} ITEMS
                INNER JOIN PARCEL_SEM_VW.ITEM_SCAN_EVENT_FCT SCAN
                ON ITEMS.PIN = SCAN.ASSOCIATED_PIN
                INNER JOIN DL_SQ_PRD_INT.LKP_KEY_SCAN LKP
                ON SCAN.SCAN_EVENT_CODE = LKP.SCAN_EVENT_CODE
                WHERE LKP.SCAN_TYPE_L1 = 'RTS' )
                )
                WITH DATA UNIQUE PRIMARY INDEX ({self.key_column}, ID) 
                ON COMMIT PRESERVE ROWS;
            """,

            "###get_all_0172_scans" : f"""
            Create multiset volatile table missort_0172 as (sel DISTINCT(item_id) from ALL_SCANS where scan_event_code='0172')
            WITH DATA UNIQUE PRIMARY INDEX (item_id)                 
            ON COMMIT PRESERVE ROWS;
            """,
        
            "###potention_missorts+POTENTIAL_MISSORTS" : f"""
                CREATE MULTISET VOLATILE TABLE POTENTIAL_MISSORTS AS (
                SEL {self.key_column}                    
                FROM
                ALL_SCANS                     
                GROUP BY
                {self.key_column} HAVING  MAX(ID) > 2                         
                OR COUNT(DISTINCT SITE_ID) > 1                 
                )
                WITH DATA UNIQUE PRIMARY INDEX ({self.key_column})                 
                ON COMMIT PRESERVE ROWS;

            """,
            
            "###keep_potential_missort_scans_only+ALL_SCANS" : f"""
                DELETE FROM ALL_SCANS
                WHERE NOT EXISTS (
                    SEL 1 FROM POTENTIAL_MISSORTS T 
                    WHERE ALL_SCANS.{self.key_column} = T.{self.key_column}
                )
            """,

            "###create a temp 1 table":f"""  
                CREATE MULTISET VOLATILE TABLE TempTable1 AS (
                SEL T1.PIN,
                    T1.ITEM_ID,
                    T1.ID AS T1_ID,
                    T2.ID AS T2_ID,
                    T1.SITE_ID AS SITE_1,
                    T1.SCAN_DTM_UTC AS SCAN_DTM_UTC_1,
                    T1.SCAN_DTM_LOCAL AS SCAN_DTM_LOC_1, 
                    T1.SCAN_EVENT_CODE AS SCAN_CODE_1,
                    T1.SCAN_WORK_CENTRE AS SCAN_WC_1,
                    T2.SITE_ID AS SITE_2,
                    T2.SCAN_DTM_UTC AS SCAN_DTM_UTC_2, 
                    T2.SCAN_DTM_LOCAL AS SCAN_DTM_LOC_2,
                    T2.SCAN_EVENT_CODE AS SCAN_CODE_2,
                    T2.SCAN_WORK_CENTRE AS SCAN_WC_2
                FROM ALL_SCANS T1                     
                INNER JOIN ALL_SCANS T2                         
                ON T1.{self.key_column} = T2.{self.key_column}                            
                AND T1.SITE_ID <> T2.SITE_ID                             
                AND T1.ID < T2.ID
                ) WITH DATA PRIMARY INDEX (T1_ID, T2_ID) ON COMMIT PRESERVE ROWS; 
            """,
            "###create a temp 2 table":f"""
            CREATE MULTISET VOLATILE TABLE TempTable2 AS (
                SEL T1.PIN,
                    T1.ITEM_ID,
                    T1.T1_ID,
                    T1.T2_ID,
                    T3.ID AS T3_ID,
                    T1.SITE_1,
                    T1.SCAN_DTM_UTC_1,
                    T1.SCAN_DTM_LOC_1, 
                    T1.SCAN_CODE_1,
                    T1.SCAN_WC_1,
                    T1.SITE_2,
                    T1.SCAN_DTM_UTC_2, 
                    T1.SCAN_DTM_LOC_2,
                    T1.SCAN_CODE_2,
                    T1.SCAN_WC_2,
                    T3.SITE_ID AS SITE_3,
                    T3.SCAN_DTM_UTC AS SCAN_DTM_UTC_3,
                    T3.SCAN_DTM_LOCAL AS SCAN_DTM_LOC_3,
                    T3.SCAN_EVENT_CODE AS SCAN_CODE_3,
                    T3.SCAN_WORK_CENTRE AS SCAN_WC_3
                FROM TempTable1 T1                     
                INNER JOIN ALL_SCANS T3                         
                ON T1.{self.key_column} = T3.{self.key_column}                           
                AND T1.SITE_1 = T3.SITE_ID                             
                AND T1.T2_ID < T3.ID
                ) WITH DATA PRIMARY INDEX (T1_ID, T2_ID, T3_ID) ON COMMIT PRESERVE ROWS;
            """,
            
            "###get_missorts+MISSORT_ITEMS" : f"""
                CREATE VOLATILE TABLE MISSORT_ITEMS AS (
                    SELECT T1.PIN,
                            T1.ITEM_ID,
                            T1.SITE_1,
                            T1.SCAN_DTM_UTC_1,
                            T1.SCAN_DTM_LOC_1, 
                            T1.SCAN_CODE_1,
                            T1.SCAN_WC_1,
                            T1.SITE_2,
                            T1.SCAN_DTM_UTC_2, 
                            T1.SCAN_DTM_LOC_2,
                            T1.SCAN_CODE_2,
                            T1.SCAN_WC_2,
                            T1.SITE_3,
                            T1.SCAN_DTM_UTC_3,
                            T1.SCAN_DTM_LOC_3,
                            T1.SCAN_CODE_3,
                            T1.SCAN_WC_3,
                            T4.SITE_ID AS SITE_4,
                            T4.SCAN_DTM_UTC AS SCAN_DTM_UTC_4,
                            T4.SCAN_DTM_LOCAL AS SCAN_DTM_LOC_4,
                            T4.SCAN_EVENT_CODE AS SCAN_CODE_4,
                            T4.SCAN_WORK_CENTRE AS SCAN_WC_4
                    FROM TempTable2 T1                     
                    LEFT JOIN ALL_SCANS T4                         
                    ON T1.{self.key_column}= T4.{self.key_column}                            
                    AND T1.T3_ID + 1 = T4.ID 
                    QUALIFY ROW_NUMBER() OVER (PARTITION BY T1.PIN                         
                                                ORDER BY T1.SCAN_DTM_UTC_1, T1.SCAN_DTM_UTC_2, T1.SCAN_DTM_UTC_3) = 1
                    ) WITH DATA PRIMARY INDEX ({self.key_column}) ON COMMIT PRESERVE ROWS;
            """,
           
            "insert_missort_into_check_scan_master_1" : f"""
                INSERT INTO {self.check_records_tb}
                    SEL PIN, ITEM_ID
                        ,  CASE 
                            WHEN SITE_4 IS NULL THEN {projConst.PAT_CAT_SEQ['MISSORT_WC_UNKNOWN']}
                            ELSE {projConst.PAT_CAT_SEQ['MISSORT_WC_KNOWN']}
                        END PAT_CAT_SEQ
                        , CAST(NULL AS TIMESTAMP(0)) CUTOFF_DTM, 'LT' CHECK_RESULT_CODE
                        , CASE
                            WHEN SITE_4 IS NULL THEN 26
                            ELSE 27
                        END
                        , SCAN_WC_1, SCAN_DTM_LOC_1, SCAN_DTM_UTC_1, SCAN_CODE_1
                        , 1 DP0_IND, 1 DP1_IND
                    FROM MISSORT_ITEMS
                    WHERE SITE_2 <> SITE_4
                        OR SITE_4 IS NULL
            """,
            
            "insert_missort_into_check_scan_master_2" : f"""
                INSERT INTO {self.check_records_tb}
                    SEL PIN, ITEM_ID
                        , {projConst.PAT_CAT_SEQ['MISSORT_WC_KNOWN']}
                        , CAST(NULL AS TIMESTAMP(0)) CUTOFF_DTM, 'LT', 27
                        , SCAN_WC_2, SCAN_DTM_LOC_2, SCAN_DTM_UTC_2, SCAN_CODE_2
                        , 1 DP0_IND, 1 DP1_IND
                    FROM MISSORT_ITEMS
                    WHERE SITE_2 = SITE_4
            """,            
            
            #"del_exists_from_missort_master" : f"""
            #    MERGE INTO {self.pat_missort_tb} T1
            #    USING MISSORT_ITEMS T2
            #        ON T1.{self.key_column} = T2.{self.key_column}
            #    WHEN MATCHED THEN DELETE
            #""",
            
            #"insert_into_missort_master" : f"""
            #    INSERT INTO {self.pat_missort_tb}
            #    SEL T.*, CAST(CURRENT_TIMESTAMP(0) AS TIMESTAMP(0))
            #    FROM MISSORT_ITEMS T
            #"""
        }
        self.query_runner(qs)
        self.drop_multiple_tables(['ALL_SCANS', 'POTENTIAL_MISSORTS','TempTable2','TempTable1'])

    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    
    def overrite_missort_gde(self):
        qs = {
            # overrite gde missort to 430
            "get_item_for_gde_not_9999" : f"""
                CREATE MULTISET VOLATILE TABLE MISSORT_GDE AS 
                (SEL distinct(A.ITEM_ID) FROM {self.pat_scan_tb} A 
                INNER JOIN {projConst.FCT_ITEM} B 
                ON A.ITEM_ID = B.ITEM_ID 
                WHERE RECEIPT_SCAN_CODE='0910' 
                AND ((EXPECTED_F_PROC_SITE ='I086' AND F_PLT_F_PROC_SITE <> 'I086') OR (EXPECTED_F_PROC_SITE ='I029' AND F_PLT_F_PROC_SITE <> 'I029'))
                AND A.DP0_CAT_ID <> 9999)
                WITH DATA PRIMARY INDEX (ITEM_ID)
                ON COMMIT PRESERVE ROWS;
            """,
            
            "hard_code_430_in_pat_scan_tb" : f"""
                UPDATE {self.pat_scan_tb} SET DP0_CAT_ID = 430 WHERE ITEM_ID IN (SEL * FROM MISSORT_GDE);
            """,

            "hard_code_430_in_master_tb" : f"""
                UPDATE {self.item_master_tb} SET DP0_CAT_ID = 430 WHERE ITEM_ID IN (SEL * FROM MISSORT_GDE);
            """,

            "drop_tb_MISSORT_GDE" : f"""
                DROP TABLE MISSORT_GDE;
            """
            
        }
        self.query_runner(qs)
    
    def categorization(self):
        
        # categorize items for the logic common for dp0 and dp1
        qs = {
            # specialcase for both dp0 and dp1
            # special and still on-time ones will be updated in later steps. 
            "get_special_case_items" : f"""
                CREATE MULTISET VOLATILE TABLE SPECIAL_ITEMS AS (
                    SEL {self.key_column}, PAT_CAT_SEQ
                    FROM {self.check_records_tb}
                    WHERE PAT_CAT_SEQ in {projConst.SPECIAL_CASE_RANGE[0],projConst.SPECIAL_CASE_RANGE[1],projConst.SPECIAL_CASE_RANGE[2]}
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY {self.key_column}
                        ORDER BY PAT_CAT_SEQ
                    ) = 1
                ) WITH DATA UNIQUE PRIMARY INDEX ({self.key_column})
                ON COMMIT PRESERVE ROWS;
            """,
            
            "item_with_special_cases" : f"""
                MERGE INTO {self.item_master_tb} T1
                USING SPECIAL_ITEMS T2
                    ON T1.{self.key_column} = T2.{self.key_column}
                        AND T1.DP0_CAT_ID IS NULL
                WHEN MATCHED THEN
                    UPDATE SET
                        DP0_CAT_ID = T2.PAT_CAT_SEQ
                        , DP1_CAT_ID = T2.PAT_CAT_SEQ
            """,
            
        }
        self.query_runner(qs)
        
        # categorize items for the logic which may be different for dp0 and dp1
        def categorize_dp0_dp1(dp : str): 
            qs = {
                f"{dp}_ontime" : f"""
                    MERGE INTO {self.item_master_tb} T1
                    USING {self.item_master_tb} T2
                        ON T1.{self.key_column} = T2.{self.key_column}
                            AND T2.{dp} = 1
                    WHEN MATCHED THEN
                        UPDATE SET
                            {dp}_CAT_ID = {projConst.PAT_CAT_SEQ['ONTIME']}
                    """,
                
                f"{dp}_ontime_is_null" : f"""
                    MERGE INTO {self.item_master_tb} T1
                    USING {self.item_master_tb} T2
                        ON T1.{self.key_column} = T2.{self.key_column}
                            AND T2.{dp} IS NULL
                    WHEN MATCHED THEN
                        UPDATE SET
                            {dp}_CAT_ID = {projConst.PAT_CAT_SEQ['ONTIME_IS_NULL']}
                """,
                
                # Find the last on-time scan, this should include
                # all items. 
                f"last_ontime_{dp}" : f"""
                    CREATE MULTISET VOLATILE TABLE LAST_ONTIME_{dp} AS (
                        SEL ITEMS.{self.key_column}, SCANS.PAT_CAT_SEQ
                        FROM {self.check_records_tb} SCANS
                        INNER JOIN {self.item_master_tb} ITEMS
                            ON SCANS.{self.key_column} = ITEMS.{self.key_column}
                                AND ITEMS.{dp} = 0                          -- dp0/dp1 is late 
                                AND SCANS.{dp}_IND = 1                      -- only include the row which is used for the dp0/dp1
                                AND SCANS.CHECK_RESULT_CODE = 'OT'          -- induction is always OT, so all items will 
                                AND SCANS.PAT_CAT_SEQ NOT IN ({projConst.PAT_SEQ_NO_NOT_FOR_CAT})
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY SCANS.{self.key_column}
                            ORDER BY SCANS.PAT_CAT_SEQ DESC
                        ) = 1
                    ) WITH DATA UNIQUE PRIMARY INDEX ({self.key_column})
                    ON COMMIT PRESERVE ROWS;
                """,
                
                # 
                f"first_late_after_last_ontime_{dp}" : f"""
                    CREATE MULTISET VOLATILE TABLE FIRST_LATE_{dp} AS (
                        SEL SCANS.{self.key_column}, LO.PAT_CAT_SEQ OT_SEQ
                            , SCANS.PAT_CAT_SEQ LT_SEQ
                        FROM {self.check_records_tb} SCANS
                        INNER JOIN LAST_ONTIME_{dp} LO
                            ON SCANS.{self.key_column} = LO.{self.key_column}
                                AND SCANS.CHECK_RESULT_CODE = 'LT'
                                AND SCANS.PAT_CAT_SEQ > LO.PAT_CAT_SEQ
                                AND SCANS.{dp}_IND = 1
                                AND SCANS.PAT_CAT_SEQ NOT IN ({projConst.PAT_SEQ_NO_NOT_FOR_CAT})
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY SCANS.{self.key_column}
                            ORDER BY SCANS.PAT_CAT_SEQ
                        ) = 1
                    ) WITH DATA UNIQUE PRIMARY INDEX ({self.key_column})
                    ON COMMIT PRESERVE ROWS;
                """,
                
                f"un_between_ot_and_lt_{dp}" : f"""
                    CREATE MULTISET VOLATILE TABLE UNKNOWNS_{dp} AS (
                        SEL SCANS.{self.key_column}
                            , SCANS.PAT_CAT_SEQ UN_SEQ
                        FROM {self.check_records_tb} SCANS
                        INNER JOIN FIRST_LATE_{dp} T1
                            ON SCANS.{self.key_column} = T1.{self.key_column}
                                AND SCANS.CHECK_RESULT_CODE = 'UN'
                                AND SCANS.PAT_CAT_SEQ BETWEEN T1.OT_SEQ AND T1.LT_SEQ
                                AND SCANS.{dp}_IND = 1
                                AND SCANS.PAT_CAT_SEQ NOT IN ({projConst.PAT_SEQ_NO_NOT_FOR_CAT})
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY SCANS.{self.key_column}
                            ORDER BY SCANS.PAT_CAT_SEQ
                        ) = 1
                    ) WITH DATA UNIQUE PRIMARY INDEX ({self.key_column})
                    ON COMMIT PRESERVE ROWS;
                """,
                
                "FLALO_but_with_unknowns" : f"""
                    MERGE INTO {self.item_master_tb} T1
                    USING UNKNOWNS_{dp} T2
                        ON T1.{self.key_column} = T2.{self.key_column}
                            AND T1.{dp}_CAT_ID IS NULL
                    WHEN MATCHED THEN
                        UPDATE SET
                            {dp}_CAT_ID = -1 * T2.UN_SEQ
                """,
                
                "first_late_after_last_on_time" : f"""
                    MERGE INTO {self.item_master_tb} T1
                    USING FIRST_LATE_{dp} T2
                        ON T1.{self.key_column} = T2.{self.key_column}
                            AND T1.{dp}_CAT_ID IS NULL
                    WHEN MATCHED THEN
                        UPDATE SET
                            {dp}_CAT_ID = T2.LT_SEQ
                """,
                
                f"rest_{dp}" : f"""
                    CREATE MULTISET VOLATILE TABLE REST_{dp} AS (
                        SEL LO.{self.key_column}
                            , COALESCE(SCANS.PAT_CAT_SEQ, {projConst.PAT_CAT_SEQ['UNKNOWN']}) REST_SEQ
                        FROM LAST_ONTIME_{dp} LO
                        LEFT JOIN {self.check_records_tb} SCANS
                            ON SCANS.{self.key_column} = LO.{self.key_column}
                                AND SCANS.{dp}_IND = 1
                                AND SCANS.PAT_CAT_SEQ > LO.PAT_CAT_SEQ
                                AND SCANS.CHECK_RESULT_CODE = 'UN'
                                AND SCANS.PAT_CAT_SEQ NOT IN ({projConst.PAT_SEQ_NO_NOT_FOR_CAT})
                        QUALIFY ROW_NUMBER() OVER (
                            PARTITION BY LO.{self.key_column}
                            ORDER BY SCANS.PAT_CAT_SEQ
                        ) = 1
                    ) WITH DATA UNIQUE PRIMARY INDEX ({self.key_column})
                    ON COMMIT PRESERVE ROWS;
                """,
                
                "merge_into_all_the_rest" : f"""
                    MERGE INTO {self.item_master_tb} T1
                    USING REST_{dp} T2
                        ON T1.{self.key_column} = T2.{self.key_column}
                            AND T1.{dp}_CAT_ID IS NULL
                    WHEN MATCHED THEN
                        UPDATE SET
                            {dp}_CAT_ID = -1 * T2.REST_SEQ
                """,
            }
            
            self.query_runner(qs)
            tb_to_delete = [f'LAST_ONTIME_{dp}', f'FIRST_LATE_{dp}', f'UNKNOWNS_{dp}'
                            , f'rest_{dp}']
            self.drop_multiple_tables(tb_to_delete)
        
        categorize_dp0_dp1('dp0')
        categorize_dp0_dp1('dp1')
        
        # merge to master            
        qs = {
          
            "combine_scan_and_cat" : f"""
                CREATE MULTISET VOLATILE TABLE SCAN_AND_CAT AS (
                    SEL SCAN.*, ITEM.DP0_CAT_ID, ITEM.DP1_CAT_ID, {self.fyfw} FYFW
                    FROM {self.check_records_tb} SCAN
                    INNER JOIN {self.item_master_tb} ITEM
                        ON SCAN.{self.key_column} = ITEM.{self.key_column}
                ) WITH DATA UNIQUE PRIMARY INDEX ({self.key_column}, PAT_CAT_SEQ, DP0_IND)
                ON COMMIT PRESERVE ROWS;
            """,
            
            "delete_exists_from_master" : f"""
                DELETE {self.pat_scan_tb}
                FROM {self.item_master_tb} T
                WHERE {self.pat_scan_tb}.{self.key_column} = T.{self.key_column}
            """,
            
            "insert_into_master" : f"""
                INSERT INTO {self.pat_scan_tb}                   -- change to 
                SEL T.*, CAST(CURRENT_TIMESTAMP(0) AS TIMESTAMP(0)) 
                FROM SCAN_AND_CAT T
            """,            
        }
        self.query_runner(qs)
        
        self.drop_multiple_tables(['SPECIAL_ITEMS', 'SCAN_AND_CAT'])
    
    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def overrite_missort(self):
        qs = {

            # overrite missort_known to 410
            "get_item_for_MISSORT_KNOWN" : f"""
                CREATE MULTISET VOLATILE TABLE GET_MISSORT_KNOWN AS (
                sel DISTINCT(a.ITEM_ID) from {self.pat_scan_tb} a 
                INNER JOIN {self.pat_scan_tb} b on a.item_id = b.item_id and a.pin=b.pin 
                where a.pat_cat_seq = '420'
                and a.related_scan_wc <> b.related_scan_wc 
                and b.pat_cat_seq='700' AND b.DP0_IND ='1'
                and b.check_result_code = 'LT' AND 
                b.DP0_CAT_ID NOT IN ('9999','800','9997','300','400','150','160','405','-300','-400','-500','-600','-700') AND a.ITEM_ID IN (SEL * FROM missort_0172))
                WITH DATA PRIMARY INDEX (item_id) ON COMMIT PRESERVE ROWS;
            """,
            
            "hard_code_410_in_pat_scan_tb" : f"""
                UPDATE {self.pat_scan_tb} SET DP0_CAT_ID = 410 WHERE ITEM_ID IN (SEL * FROM GET_MISSORT_KNOWN);
            """,

            "hard_code_410_in_master_tb" : f"""
                UPDATE {self.item_master_tb} SET DP0_CAT_ID = 410 WHERE ITEM_ID IN (SEL * FROM GET_MISSORT_KNOWN);
            """,

            "REMOVE_item_for_MISSORT_KNOWN" : f"""
                CREATE MULTISET VOLATILE TABLE LEFT_OUT AS (
                SEL DISTINCT(ITEM_ID) FROM MISSORT_ITEMS
                EXCEPT
                SEL DISTINCT(ITEM_ID) FROM GET_MISSORT_KNOWN
                ) WITH DATA PRIMARY INDEX (item_id) ON COMMIT PRESERVE ROWS;
            """,

            "GET_items_for_missort_unknown" :f"""
                Create multiset volatile table GET_MISSORT_UNKNOWN as (
                sel item_id from {self.pat_scan_tb} where item_id in (sel * from LEFT_OUT) and pat_cat_seq='700' AND DP0_IND ='1'
                and dp0_cat_id NOT in ('9999','800','9997','300','400','150','160','405','-300','-400','-500','-600','-700'))
                WITH DATA PRIMARY INDEX (item_id) ON COMMIT PRESERVE ROWS;
            
            """,
            
            "hard_code_430_in_pat_scan_tb" : f"""
                UPDATE {self.pat_scan_tb} SET DP0_CAT_ID = 430 WHERE ITEM_ID IN (SEL * FROM GET_MISSORT_UNKNOWN);
            """,

            "hard_code_430_in_master_tb" : f"""
                UPDATE {self.item_master_tb} SET DP0_CAT_ID = 430 WHERE ITEM_ID IN (SEL * FROM GET_MISSORT_UNKNOWN);
            """,

            "drop_tb_MISSORT_0172" : f"""
                DROP TABLE missort_0172;
            """,

            "drop_tb_GET_MISSORT_KNOWN" : f"""
                DROP TABLE GET_MISSORT_KNOWN;
            """,

            "drop_tb_GET_MISSORT_UNKNOWN" : f"""
                DROP TABLE GET_MISSORT_UNKNOWN;
            """,
            "drop_tb_LEFT_OUT" : f"""
                DROP TABLE LEFT_OUT;
            """,
            "drop_tb_MISSORT_ITEMS" : f"""
                DROP TABLE MISSORT_ITEMS;
            """
            
            
        }
        self.query_runner(qs)

        
    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def update_spdm_tables(self): 
        
        spdm_table_lst = [projConst.SPDM_DOMESTIC, projConst.SPDM_INBOUND, projConst.SPDM_OUTBOUND]
        for tb in spdm_table_lst:
            qs = {
                f"update_final_result_int_{tb}" : f"""
                    -- not using merge, as DOMESTIC has multilevel PPI
                    -- which not sure how to set the condition in 
                    -- merge statement. 
                    UPDATE {tb}
                    FROM {self.item_master_tb} T2
                    SET CLOCK_START_DT = T2.CLOCK_START_DT
                        , STD_DELIVER_DATE_DP0 = T2.STD_DELIVER_DATE_DP0
                        , STD_DELIVER_DATE_DP1 = T2.STD_DELIVER_DATE_DP1
                        , PAT_CAT_SEQ_DP0 = T2.DP0_CAT_ID
                        , PAT_CAT_SEQ_DP1 = T2.DP1_CAT_ID
                        , LAST_REFRESH_DTM = CAST(CURRENT_TIMESTAMP(0) AS TIMESTAMP(0))
                    WHERE {tb}.ITEM = T2.{self.key_column}
                        AND {tb}.FYFW = {self.fyfw}
                """,
            
          
            }
            
            self.query_runner(qs)
            
    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def update_flash_tables(self):
            
            qs = {
                f"update_final_result_int_{projConst.FCT_ITEM}" : f"""
                    MERGE INTO {projConst.FCT_ITEM} T1
                    USING {self.item_master_tb} T2
                        ON T1.ITEM_ID = T2.ITEM_ID
                    WHEN MATCHED THEN
                        UPDATE SET
                            -- ESTIMATED_DELIVERY_DT = T2.STD_DELIVER_DATE_DP0,
                            PAT_CAT_SEQ_DP0 = T2.DP0_CAT_ID
                            , PAT_CAT_SEQ_DP1 = T2.DP1_CAT_ID
                            , LAST_REFRESH_DTM = CAST(CURRENT_TIMESTAMP(0) AS TIMESTAMP(0));
                """,

                "create volatile table to map fyfw": f"""
                    CREATE MULTISET VOLATILE TABLE FYFW_VALUES AS (
                    SELECT
                        a.ITEM_ID,
                        CONCAT(
                        TRIM(YEAR(COALESCE(b.SPE_ODT, CAST(CAST(b.RECEIPT_SCAN_DTM_LOC AS DATE) AS TIMESTAMP(0))))),
                        LPAD(TRIM(WEEKNUMBER_OF_YEAR(COALESCE(b.SPE_ODT, CAST(CAST(b.RECEIPT_SCAN_DTM_LOC AS DATE) AS TIMESTAMP(0))))), 2, '0')
                        ) AS FYFW,
                        a.PAT_CAT_SEQ,
                        a.DP0_IND
                    FROM
                        {self.pat_scan_tb} a
                    INNER JOIN
                        {projConst.FCT_ITEM}  b ON a.ITEM_ID = b.ITEM_ID
                    WHERE
                        a.FYFW = 0
                    ) WITH DATA PRIMARY INDEX (ITEM_ID, PAT_CAT_SEQ, DP0_IND) ON COMMIT PRESERVE ROWS;    
                """,

                "update DL_SQ_PRD_INT.FCT_PAT_FLASH_SCAN table with FYFW":f"""
                    MERGE INTO {self.pat_scan_tb} T1
                    USING FYFW_VALUES T2
                        ON T1.ITEM_ID = T2.ITEM_ID AND
                        T1.PAT_CAT_SEQ = T2.PAT_CAT_SEQ AND
                        T1.DP0_IND = T2.DP0_IND
                    WHEN MATCHED THEN
                        UPDATE SET
                            FYFW = T2.FYFW;

                """
            
            }
            self.query_runner(qs)
            self.drop_multiple_tables(['FYFW_VALUES'])
    
    def initiate_tables(self, mode : str):

        key_column = 'ITEM_ID' if mode == 'FLASH' else 'PIN'
        if not is_table_exist(self.cur, self.check_records_tb):
            self.cur.execute(just_ddls.check_records_ddl(self.check_records_tb, key_column))
            
        if not is_table_exist(self.cur, projConst.DUP_OPLT_CUTOFF):
            self.cur.execute(just_ddls.dup_oplt_cutoff(projConst.DUP_OPLT_CUTOFF, key_column))
            
        if not is_table_exist(self.cur, projConst.DUP_DPLT_CUTOFF):
            self.cur.execute(just_ddls.dup_oplt_cutoff(projConst.DUP_DPLT_CUTOFF, key_column))
            
        if not is_table_exist(self.cur, self.pat_scan_tb):
            self.cur.execute(just_ddls.fct_pat_scan_spe(self.pat_scan_tb, key_column))
            
        if not is_table_exist(self.cur, self.pat_missort_tb):
            self.cur.execute(just_ddls.missort_scans(self.pat_missort_tb, key_column))


    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def spe_processes(self, fyfw : int):
        self.fyfw = fyfw
        str_fyfw = str(fyfw)
        self.fy_wfw = str_fyfw[:4]+"-W"+str_fyfw[4:]
        self.cur.execute(f"""
            SEL FIRST_CAL_DATE_OF_FISC_WEEK
                , LAST_CAL_DATE_OF_FISC_WEEK
            FROM {SQConstants.SQ_WEEK_VW}
            WHERE FISCAL_YEAR_WEEK_NUMBER = {fyfw}
        """)
        self.first_day, self.last_day = self.cur.fetchone()

        self.spe_prepare_items()
        self.spe_extract(self.fy_wfw)
        self.spe_clock_start_dt()
        self.spe_std_del_dt()
        self.spe_induction()
        self.check_special_case()
        self.originating_plt_last_ilc()
        self.originating_plt_first_ilc() # last_ilc needs to be run first to identify if the plant is expected originating plant. 
        self.dispatch_late('o')
        self.destination_plt_last_ilc()        
        self.destination_plt_first_ilc()
        self.dispatch_late('d')
        self.dss()
        self.dss_ontime_del_late()
        self.missort()
        self.categorization()
        self.overrite_missort()
        #self.overrite_missort_gde()
        self.update_spdm_tables()
        self.cur.execute(f'DEL FROM {self.check_records_tb} ALL')
        self.cur.execute(f'DROP TABLE {self.item_master_tb}')
        # self.keep_wip_vtb(self.check_records_tb, 'DL_SQ_PRD_WRK.A28_CHECK_RECORDS_DSS', 'ITEM_ID')
        # self.keep_wip_vtb(self.item_master_tb, 'DL_SQ_PRD_WRK.A28_ITEM_MASTER_DSS', 'ITEM_ID')
        # self.item_master_tb = 'DL_SQ_PRD_WRK.A28_ITEM_MASTER_DSS'
        # self.check_records_tb = 'DL_SQ_PRD_WRK.A28_CHECK_RECORDS_DSS'

    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def spe_run(self, start_fyfw : int =None, end_fyfw : int = None):
        
        self.key_column = 'PIN'
        self.fct_origin_cutoff = projConst.FCT_ORIGIN_CUTOFF
        self.origin_cutoff_smry = projConst.ORIGIN_CUTOFF_SMRY
        self.edw_key_column = 'ASSOCIATED_PIN'
        self.pat_scan_tb = projConst.FCT_PAT_SPDM_SCAN
        self.pat_missort_tb = projConst.FCT_PAT_SPDM_MISSORT_SCAN
        self.initiate_tables('SPE')
        if start_fyfw is None:
            start_fyfw = end_fyfw = get_latest_spe_wks(self.cur)[0][0]
        
        # considering the duplicated ITEM_ID among weeks 
        # process SPE data 1 week per loop. 
        loop_week = start_fyfw
        while loop_week <= end_fyfw:
            SQLoggerM(f"{'*'*10}  Start fiscal year week {loop_week} {'*'*10}",
                log_filename=projConst.LOG_FILE,
                proj_desc=projConst.PROJECT_NAME,
                proj_id=projConst.PROJECT_NO
            )
            self.spe_processes(loop_week)
            loop_week = self.spe_get_next_fyfw(loop_week, 1)

    @SQLogger(
        log_filename=projConst.LOG_FILE,
        proj_desc=projConst.PROJECT_NAME,
        proj_id=projConst.PROJECT_NO
    )
    def flash_process(self):
        
        self.key_column = 'ITEM_ID'   
        self.edw_key_column = 'ITEM_ID'
        self.pat_scan_tb = projConst.FCT_PAT_FLASH_SCAN
        self.pat_missort_tb = projConst.FCT_PAT_FLASH_MISSORT_SCAN
        self.initiate_tables('FLASH')
        self.flash_extract()
        self.flash_prepare_items()
        self.flash_induction()
        self.check_special_case()
        self.originating_plt_last_ilc()
        self.originating_plt_first_ilc()
        self.dispatch_late('o')
        self.destination_plt_last_ilc()        
        self.destination_plt_first_ilc()
        self.dispatch_late('d')
        self.dss()
        self.dss_ontime_del_late()
        self.missort()
        self.categorization()
        self.overrite_missort()
        #self.overrite_missort_gde()
        self.update_flash_tables()
        self.cur.execute(f'DEL FROM {self.check_records_tb} ALL')
        self.cur.execute(f'DROP TABLE {self.item_master_tb}')
        
if __name__ == "__main__":
    
    with DataSourceConnection() as con:
        cur = con.cursor()
        pat = PATII(cur)
        pat.flash_process()
        