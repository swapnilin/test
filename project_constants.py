"""This file defines the constants required for this project. 
"""
import argparse
import logging
import socket
import os
from sqtoolbox.gen.sqconstants import SQConstants



class Constants(object):
    
    def __init__(self, args=None) -> None:
        
        cli_args = self.commandline_arguments(args)
        
        self.LOG_INSTANCE_TAG = cli_args.instance_tag
        self.HOSTNAME = socket.gethostname()
        self.PROJECT_NAME = os.path.basename(os.path.dirname(os.path.realpath(__file__)))

        if self.LOG_INSTANCE_TAG is not None and len(self.LOG_INSTANCE_TAG) > 0:
            self.PROJECT_NAME = self.PROJECT_NAME + f' ({self.LOG_INSTANCE_TAG})'
            
        self.PROJECT_PATH = os.path.dirname(os.path.realpath(__file__))
        self.RUNNING_FOLDER = os.getcwd()
        self.PROJECT_NO = self.PROJECT_NAME.split('-')[0]
        self.RUNNING_ACCOUNT = os.getlogin()

        # key parameters
        self.RUN_MODE = "DEV" if cli_args.develop else "PRD"            # Running mode, DEV: for development, PRD: for production
        self.SHOW_CONSTANTS = cli_args.show_constants
        self.LOG_FILE = None if cli_args.show_log else f'.\\{self.PROJECT_NAME}.log' 
        self.LOG_LEVEL = logging.INFO
        self.DAYS_IN_ONE_LOOP = 30
        self.SPE_SCAN_SEARCH_RANGE = 90

        self.DATA_CONNECTION_PARA = 'DEFAULT'
        self.EMAIL_CONFIG_PROFILE = 'SQINFO'
        self.TECH_SUPPORT_RECIPIENTS = ['rani.adhaduk@canadapost.ca','swapnil.kangralkar@canadapost.ca','jonathan.mahilrajan@canadapost.ca','ian.yang@canadapost.postescanada.ca']
        self.NOTIFICATION_RECIPIENTS = self.TECH_SUPPORT_RECIPIENTS if cli_args.tech_email else [SQConstants.EMAIL_SQ_NOTE_CHANNEL]
        self.DATA_CONNECTION_PARA = cli_args.data_source
        
        # constants for programming control
        self.FLASH_RUN_DAYS_PER_LOOP = 5

        self.START_WEEK = cli_args.start_week
        self.END_WEEK = cli_args.end_week



        self.PAT_CAT_SEQ = {
            'INDUCTION' : 100,
            # special case seq number shall be within the SPECIAL_CASE_RANGE
            'FM' : 150,
            'CUSTOMER_MISSORT' : 160,
            'MISSORT_WC_KNOWN' : 410, #200
            'MISSORT_SCAN' : 420,     #210
            'MISSORT_WC_UNKNOWN' : 430, #220
            'UMO' : 405,# 
            # special case seq number shall be within the SPECIAL_CASE_RANGE
            'OPLT_F_ILC' : 300,
            'OPLT_L_ILC' : 400, 
            'OPLT_DISPATCH' : 450,
            'DPLT_F_ILC' : 500,
            'DPLT_L_ILC' : 600,
            'DPLT_DISPATCH' : 650, 
            'DSS' : 700,
            'DSS_OT_DL_LATE' : 800,
            'ONTIME_IS_NULL' : 9997,
            'ONTIME' : 9999,
            'UNKNOWN' : 9998
        }
        
        self.SPECIAL_CASE_RANGE = [150, 160, 405]

        self.OUTBOUND_PRODUCT_TYPES = "'TC', 'TE', 'XI', 'EU', 'XC', 'TU', 'TI', 'XE', 'EC', 'XU'"
        self.OUTBOUND_EXCHANGE_OFFICE_SITE_ID = "'I029', 'I086', 'I186'"

        self.OPLT_F_PROC_CUTOFF_TIME = '12:00:00'
        self.DPLT_F_PROC_CUTOFF_TIME = '12:00:00'
        self.DEPOT_CUTOFF_TIME = '11:00:00'
        self.DELIVERY_CUTOFF_TIME = '17:00:00'
        self.PAT_SEQ_NO_NOT_FOR_CAT = '410,420,430,450,650'
        self.FSA_REG_EXP = '^[ABCEGHJ-NPRSTVXY]\d[ABCEGHJ-NPRSTV-Z]'
        self.LDU_REG_EXP = '\d[ABCEGHJ-NPRSTV-Z]\d$'
        self.UPDATE_MODE = cli_args.update_mode

        # Source tables
        self.BUSINESS_DAY = 'DL_SQ_PRD_INT.VW_LKP_NEXT_PREV_BUSDAY'
        self.SQ_SPE_PRODUCT_DIM = 'DL_SQ_PRD_SEM.VW_DIM_SPE_PRODUCT'
        self.SHIP_DURATION = 'DL_SQ_PRD_INT.VW_LKP_DURATION'
        self.PAT_ORIGINATING_CUTOFF = 'DL_SQ_PRD_INT.LKP_PAT_ORG_CUTOFF'
        self.PAT_DEST_CUTOFF_WC = 'DL_SQ_PRD_INT.LKP_PAT_DEST_CUTOFF_ILC'
        self.PAT_DEST_CUTOFF_CNTR = 'DL_SQ_PRD_INT.LKP_PAT_DEST_CUTOFF_CNTR'
        self.PAT_DEST_CUTOFF_FSA = 'DL_SQ_PRD_INT.LKP_PAT_DEST_CUTOFF_FSA'
        self.PAT_OUTBOUND_CUTOFF = 'DL_SQ_PRD_INT.LKP_OUTBOUND_DEST_PLANT_CUTOFF'        
        self.LEG1_END_CUTOFF = 'DL_SQ_PRD_INT.LKP_OUTBOUND_LEG1_END_CUTOFF'
        self.FCT_ITEM = f'DL_SQ_{self.RUN_MODE}_INT.VW_FCT_ITEM' #DL_SQ_{self.RUN_MODE}_INT.FCT_ITEM_DEBUG_OPSOPAT_77'  #f'DL_SQ_{self.RUN_MODE}_INT.VW_FCT_ITEM'
        self.LKP_KEY_SCAN= f'DL_SQ_{self.RUN_MODE}_INT.LKP_KEY_SCAN'
        self.FCT_SPECIAL_EVENT_SCAN = f'DL_SQ_{self.RUN_MODE}_INT.FCT_SPECIAL_EVENT_SCAN'
        self.FSA_ROLL = 'DL_SQ_PRD_INT.LKP_SPE_FSA_ROLL'
        # Output/UPdate tables 
        self.PAT_FLASH_CLOCK_START_DT = f'DL_SQ_{self.RUN_MODE}_INT.PAT_FLASH_CLOCK_START_DT'
        self.DUP_OPLT_CUTOFF = f'DL_SQ_{self.RUN_MODE}_INT.FCT_DUP_OPLT_CUTOFF'
        self.DUP_DPLT_CUTOFF = f'DL_SQ_{self.RUN_MODE}_INT.FCT_DUP_DPLT_CUTOFF'
        self.FCT_PAT_SPDM_SCAN = f'DL_SQ_{self.RUN_MODE}_INT.FCT_PAT_SPDM_SCAN'
        self.FCT_PAT_FLASH_SCAN = f'DL_SQ_{self.RUN_MODE}_INT.FCT_PAT_FLASH_SCAN' #DL_SQ_{self.RUN_MODE}_INT.FCT_PAT_FLASH_SCAN'DL_SQ_PRD_WRK.FCT_PAT_FLASH_SCAN_DEBUG
        self.FCT_PAT_SPDM_MISSORT_SCAN = f'DL_SQ_{self.RUN_MODE}_INT.FCT_PAT_SPDM_MISSORT_SCAN'
        self.FCT_PAT_FLASH_MISSORT_SCAN = f'DL_SQ_{self.RUN_MODE}_INT.FCT_PAT_FLASH_MISSORT_SCAN'
        self.FCT_ORIGIN_CUTOFF=f'DL_SQ_{self.RUN_MODE}_INT.FCT_ORIGIN_CUTOFF'
        self.ORIGIN_CUTOFF_SMRY=f'DL_SQ_{self.RUN_MODE}_INT.ORIGIN_CUTOFF_SMRY'
        
        # source and update
        self.SPDM_DOMESTIC = f'DL_SQ_{self.RUN_MODE}_INT.FCT_SPDM_DOMESTIC'
        self.SPDM_INBOUND = f'DL_SQ_{self.RUN_MODE}_INT.FCT_SPDM_INBOUND'
        self.SPDM_OUTBOUND = f'DL_SQ_{self.RUN_MODE}_INT.FCT_SPDM_OUTBOUND'
        self.SPDM_FLASH = f'DL_SQ_{self.RUN_MODE}_INT.FCT_SPDM_FLASH'
        
        self.STV_DEPARTURE_SCAN_CODE = '0410'
        self.STV_DEPARTURE_CUTOFF = 60 # in minutes
        self.FLASH_DAYS_RANGE = 30 #10 #30
    def commandline_arguments(self, args):
        
        parser = argparse.ArgumentParser()
        parser.add_argument('-s', '--start_week', type = int
                            , help="start week in formay yyyyww (for spe mode). default start week is latest spe week. ")
        parser.add_argument('-e', '--end_week', type = int
                            , help="end week in formay yyyyww (for spe mode). default end week is latest spe week. ")
        parser.add_argument('-l', '--show_log', default=False, action='store_true'
                            , help='Show log on terminal. Default will write to log file.')
        parser.add_argument('-d', '--develop', default=False, action='store_true'
                            , help="Related data source/target will be in 'DEV'. Default is 'PRD'.")
        parser.add_argument('-t', '--tech_email', default=False, action='store_true'
                            , help="All email will be sent to technical support only.")
        parser.add_argument('-sc', '--show_constants', default=False, action='store_true'
                            , help="Show values of project constants at begining of log.")
        parser.add_argument('-ds', '--data_source', default='DEFAULT'
                            , help='Specify config ID of data source connection.')      
        parser.add_argument('-um', '--update_mode', default='FLASH'
                            , help="'FLASH' or 'SPE'")    
        parser.add_argument('-ig', '--instance_tag', type = str
                            , help="'For logging purposes only. Add a string description to the project name of this running instance only. The tag will be logged along with the project name. Will not affect functionality.'")  
        return parser.parse_args()
    
# initiate project constant, 
# when doing test, set arguments here 
# when production, set arguments in command line and keep here with no cli_args. 
# projConst = Constant(['-t', '-l', '-d', '--check_interval', '0.1', '--check_times', '3'])  
projConst = Constants()    

