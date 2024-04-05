import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

def to_percent(col):
    '''
    Convert a column to percent, dropping some.
    '''
    return pd.to_numeric(str(col).rstrip('%'), errors='coerce')/100.0

def cleanup_numeric(df, col):
        logger.info(f"Converting {col}")
        if (col.startswith('P')):
            logger.info(col + " is percent... applying")
            df[col] = df[col].apply(to_percent)
        # Convert everything else straight to a float. SUCC/MATSUCC will be made NAs. 
        else: 
            df[col] = pd.to_numeric(df[col], errors='coerce')


def import_academies_finance(): 
    logger.info("Loading Academies finance file")
    ac = pd.read_excel('data/SFB_Academies_2021-22_download.xlsx', sheet_name=1)

    logger.info("Cleaning up data types")

    # for all columns greater than 38, convert them to numeric values, any that don't will be NaN.
    for col in ac.columns[38:]:
        ac[col] = pd.to_numeric(ac[col], errors='coerce')
    
    logger.info("Writing to parquet")
    ac.to_parquet('data/duck/academies_finance.parquet')


def import_schools_finance():
    logger.info("Loading Schools finance file")
    df = pd.read_excel('data/School_total_spend_2022-23_Full_Data_Workbook.xlsx', sheet_name=3)

    logger.info("Cleaning up data types")

    # Force column to string. 
    df['Did Not Supply flag'] = df['Did Not Supply flag'].astype('|S') 

    # Remove everything before the first space, to get rid of the I01 etc for cols 32-105
    for col in df.columns[32:106]:
        df = df.rename(columns={col: col.split(' ', 1)[1]})
    
    # Remove everything after the first :, to get rid of the calulations, this brings the data in line with
    # MATs that don't have this. 
    for col in df.columns[106:]:
        df = df.rename(columns={col: col.split(':', 1)[0]})
    
    # Rename this column as it had another different format, not caught above.
    df = df.rename(columns={"Teaching Staff  E01": "Teaching Staff"})
    
    # Remove the duplicate columns create above.
    df = df.loc[:,~df.columns.duplicated()].copy()

    logger.info("Writing to parquet")
    df.to_parquet('data/duck/schools_finance.parquet')



def import_schools_attainment():

    # Apply this schema
    schema = {'RECTYPE': 'int16', 'ALPHAIND': 'O', 'LEA': 'O', 'ESTAB': 'O', 'URN': 'O', 'SCHNAME': 'O', 'ADDRESS1': 'O', 'ADDRESS2': 'O', 'ADDRESS3': 'O', 'TOWN': 'O', 'PCODE': 'O', 'TELNUM': 'O', 'PCON_CODE': 'O', 'PCON_NAME': 'O', 'URN_AC': 'O', 'SCHNAME_AC': 'O', 'OPEN_AC': 'O', 'NFTYPE': 'O', 'ICLOSE': 'O', 'RELDENOM': 'O', 'AGERANGE': 'O', 'TAB15': 'O', 'TAB1618': 'O', 'TOTPUPS': 'O', 'TPUPYEAR': 'O', 'TELIG': 'O', 'BELIG': 'O', 'GELIG': 'O', 'PBELIG': 'O', 'PGELIG': 'O', 'TKS1AVERAGE': 'O', 'TKS1GROUP_L': 'O', 'PTKS1GROUP_L': 'O', 'TKS1GROUP_M': 'O', 'PTKS1GROUP_M': 'O', 'TKS1GROUP_H': 'O', 'PTKS1GROUP_H': 'O', 'TKS1GROUP_NA': 'O', 'PTKS1GROUP_NA': 'O', 'TFSM6CLA1A': 'O', 'PTFSM6CLA1A': 'O', 'TNOTFSM6CLA1A': 'O', 'PTNOTFSM6CLA1A': 'O', 'TEALGRP2': 'O', 'PTEALGRP2': 'O', 'TMOBN': 'O', 'PTMOBN': 'O', 'PTRWM_EXP': 'O', 'PTRWM_HIGH': 'O', 'READPROG': 'O', 'READPROG_LOWER': 'O', 'READPROG_UPPER': 'O', 'READCOV': 'O', 'WRITPROG': 'O', 'WRITPROG_LOWER': 'O', 'WRITPROG_UPPER': 'O', 'WRITCOV': 'O', 'MATPROG': 'O', 'MATPROG_LOWER': 'O', 'MATPROG_UPPER': 'O', 'MATCOV': 'O', 'PTREAD_EXP': 'O', 'PTREAD_HIGH': 'O', 'PTREAD_AT': 'O', 'READ_AVERAGE': 'O', 'PTGPS_EXP': 'O', 'PTGPS_HIGH': 'O', 'PTGPS_AT': 'O', 'GPS_AVERAGE': 'O', 'PTMAT_EXP': 'O', 'PTMAT_HIGH': 'O', 'PTMAT_AT': 'O', 'MAT_AVERAGE': 'O', 'PTWRITTA_EXP': 'O', 'PTWRITTA_HIGH': 'O', 'PTWRITTA_WTS': 'O', 'PTWRITTA_AD': 'O', 'PTSCITA_EXP': 'O', 'PTSCITA_AD': 'O', 'PTRWM_EXP_B': 'O', 'PTRWM_EXP_G': 'O', 'PTRWM_EXP_L': 'O', 'PTRWM_EXP_M': 'O', 'PTRWM_EXP_H': 'O', 'PTRWM_EXP_FSM6CLA1A': 'O', 'PTRWM_EXP_NotFSM6CLA1A': 'O', 'DIFFN_RWM_EXP': 'O', 'PTRWM_EXP_EAL': 'O', 'PTRWM_EXP_MOBN': 'O', 'PTRWM_HIGH_B': 'O', 'PTRWM_HIGH_G': 'O', 'PTRWM_HIGH_L': 'O', 'PTRWM_HIGH_M': 'O', 'PTRWM_HIGH_H': 'O', 'PTRWM_HIGH_FSM6CLA1A': 'O', 'PTRWM_HIGH_NotFSM6CLA1A': 'O', 'DIFFN_RWM_HIGH': 'O', 'PTRWM_HIGH_EAL': 'O', 'PTRWM_HIGH_MOBN': 'O', 'READPROG_B': 'O', 'READPROG_B_LOWER': 'O', 'READPROG_B_UPPER': 'O', 'READPROG_G': 'O', 'READPROG_G_LOWER': 'O', 'READPROG_G_UPPER': 'O', 'READPROG_L': 'O', 'READPROG_L_LOWER': 'O', 'READPROG_L_UPPER': 'O', 'READPROG_M': 'O', 'READPROG_M_LOWER': 'O', 'READPROG_M_UPPER': 'O', 'READPROG_H': 'O', 'READPROG_H_LOWER': 'O', 'READPROG_H_UPPER': 'O', 'READPROG_FSM6CLA1A': 'O', 'READPROG_FSM6CLA1A_LOWER': 'O', 'READPROG_FSM6CLA1A_UPPER': 'O', 'READPROG_NotFSM6CLA1A': 'O', 'READPROG_NotFSM6CLA1A_LOWER': 'O', 'READPROG_NotFSM6CLA1A_UPPER': 'O', 'DIFFN_READPROG': 'O', 'READPROG_EAL': 'O', 'READPROG_EAL_LOWER': 'O', 'READPROG_EAL_UPPER': 'O', 'READPROG_MOBN': 'O', 'READPROG_MOBN_LOWER': 'O', 'READPROG_MOBN_UPPER': 'O', 'WRITPROG_B': 'O', 'WRITPROG_B_LOWER': 'O', 'WRITPROG_B_UPPER': 'O', 'WRITPROG_G': 'O', 'WRITPROG_G_LOWER': 'O', 'WRITPROG_G_UPPER': 'O', 'WRITPROG_L': 'O', 'WRITPROG_L_LOWER': 'O', 'WRITPROG_L_UPPER': 'O', 'WRITPROG_M': 'O', 'WRITPROG_M_LOWER': 'O', 'WRITPROG_M_UPPER': 'O', 'WRITPROG_H': 'O', 'WRITPROG_H_LOWER': 'O', 'WRITPROG_H_UPPER': 'O', 'WRITPROG_FSM6CLA1A': 'O', 'WRITPROG_FSM6CLA1A_LOWER': 'O', 'WRITPROG_FSM6CLA1A_UPPER': 'O', 'WRITPROG_NotFSM6CLA1A': 'O', 'WRITPROG_NotFSM6CLA1A_LOWER': 'O', 'WRITPROG_NotFSM6CLA1A_UPPER': 'O', 'DIFFN_WRITPROG': 'O', 'WRITPROG_EAL': 'O', 'WRITPROG_EAL_LOWER': 'O', 'WRITPROG_EAL_UPPER': 'O', 'WRITPROG_MOBN': 'O', 'WRITPROG_MOBN_LOWER': 'O', 'WRITPROG_MOBN_UPPER': 'O', 'MATPROG_B': 'O', 'MATPROG_B_LOWER': 'O', 'MATPROG_B_UPPER': 'O', 'MATPROG_G': 'O', 'MATPROG_G_LOWER': 'O', 'MATPROG_G_UPPER': 'O', 'MATPROG_L': 'O', 'MATPROG_L_LOWER': 'O', 'MATPROG_L_UPPER': 'O', 'MATPROG_M': 'O', 'MATPROG_M_LOWER': 'O', 'MATPROG_M_UPPER': 'O', 'MATPROG_H': 'O', 'MATPROG_H_LOWER': 'O', 'MATPROG_H_UPPER': 'O', 'MATPROG_FSM6CLA1A': 'O', 'MATPROG_FSM6CLA1A_LOWER': 'O', 'MATPROG_FSM6CLA1A_UPPER': 'O', 'MATPROG_NotFSM6CLA1A': 'O', 'MATPROG_NotFSM6CLA1A_LOWER': 'O', 'MATPROG_NotFSM6CLA1A_UPPER': 'O', 'DIFFN_MATPROG': 'O', 'MATPROG_EAL': 'O', 'MATPROG_EAL_LOWER': 'O', 'MATPROG_EAL_UPPER': 'O', 'MATPROG_MOBN': 'O', 'MATPROG_MOBN_LOWER': 'O', 'MATPROG_MOBN_UPPER': 'O', 'READ_AVERAGE_B': 'O', 'READ_AVERAGE_G': 'O', 'READ_AVERAGE_L': 'O', 'READ_AVERAGE_M': 'O', 'READ_AVERAGE_H': 'O', 'READ_AVERAGE_FSM6CLA1A': 'O', 'READ_AVERAGE_NotFSM6CLA1A': 'O', 'READ_AVERAGE_EAL': 'O', 'READ_AVERAGE_MOBN': 'O', 'MAT_AVERAGE_B': 'O', 'MAT_AVERAGE_G': 'O', 'MAT_AVERAGE_L': 'O', 'MAT_AVERAGE_M': 'O', 'MAT_AVERAGE_H': 'O', 'MAT_AVERAGE_FSM6CLA1A': 'O', 'MAT_AVERAGE_NotFSM6CLA1A': 'O', 'MAT_AVERAGE_EAL': 'O', 'MAT_AVERAGE_MOBN': 'O', 'GPS_AVERAGE_B': 'O', 'GPS_AVERAGE_G': 'O', 'GPS_AVERAGE_L': 'O', 'GPS_AVERAGE_M': 'O', 'GPS_AVERAGE_H': 'O', 'GPS_AVERAGE_FSM6CLA1A': 'O', 'GPS_AVERAGE_NotFSM6CLA1A': 'O', 'GPS_AVERAGE_EAL': 'O', 'GPS_AVERAGE_MOBN': 'O', 'PTREAD_EXP_L': 'O', 'PTREAD_EXP_M': 'O', 'PTREAD_EXP_H': 'O', 'PTREAD_EXP_FSM6CLA1A': 'O', 'PTREAD_EXP_NotFSM6CLA1A': 'O', 'PTGPS_EXP_L': 'O', 'PTGPS_EXP_M': 'O', 'PTGPS_EXP_H': 'O', 'PTGPS_EXP_FSM6CLA1A': 'O', 'PTGPS_EXP_NotFSM6CLA1A': 'O', 'PTMAT_EXP_L': 'O', 'PTMAT_EXP_M': 'O', 'PTMAT_EXP_H': 'O', 'PTMAT_EXP_FSM6CLA1A': 'O', 'PTMAT_EXP_NotFSM6CLA1A': 'O', 'PTWRITTA_EXP_L': 'O', 'PTWRITTA_EXP_M': 'O', 'PTWRITTA_EXP_H': 'O', 'PTWRITTA_EXP_FSM6CLA1A': 'O', 'PTWRITTA_EXP_NotFSM6CLA1A': 'O', 'PTREAD_HIGH_L': 'O', 'PTREAD_HIGH_M': 'O', 'PTREAD_HIGH_H': 'O', 'PTREAD_HIGH_FSM6CLA1A': 'O', 'PTREAD_HIGH_NotFSM6CLA1A': 'O', 'PTGPS_HIGH_L': 'O', 'PTGPS_HIGH_M': 'O', 'PTGPS_HIGH_H': 'O', 'PTGPS_HIGH_FSM6CLA1A': 'O', 'PTGPS_HIGH_NotFSM6CLA1A': 'O', 'PTMAT_HIGH_L': 'O', 'PTMAT_HIGH_M': 'O', 'PTMAT_HIGH_H': 'O', 'PTMAT_HIGH_FSM6CLA1A': 'O', 'PTMAT_HIGH_NotFSM6CLA1A': 'O', 'PTWRITTA_HIGH_L': 'O', 'PTWRITTA_HIGH_M': 'O', 'PTWRITTA_HIGH_H': 'O', 'PTWRITTA_HIGH_FSM6CLA1A': 'O', 'PTWRITTA_HIGH_NotFSM6CLA1A': 'O', 'TEALGRP1': 'O', 'PTEALGRP1': 'O', 'TEALGRP3': 'O', 'PTEALGRP3': 'O', 'TSENELE': 'O', 'PSENELE': 'O', 'TSENELK': 'O', 'PSENELK': 'O', 'TSENELEK': 'O', 'PSENELEK': 'O', 'TELIG_22': 'O', 'PTFSM6CLA1A_22': 'O', 'PTNOTFSM6CLA1A_22': 'O', 'PTRWM_EXP_22': 'O', 'PTRWM_HIGH_22': 'O', 'PTRWM_EXP_FSM6CLA1A_22': 'O', 'PTRWM_HIGH_FSM6CLA1A_22': 'O', 'PTRWM_EXP_NotFSM6CLA1A_22': 'O', 'PTRWM_HIGH_NotFSM6CLA1A_22': 'O', 'READPROG_22': 'O', 'READPROG_LOWER_22': 'O', 'READPROG_UPPER_22': 'O', 'WRITPROG_22': 'O', 'WRITPROG_LOWER_22': 'O', 'WRITPROG_UPPER_22': 'O', 'MATPROG_22': 'O', 'MATPROG_LOWER_22': 'O', 'MATPROG_UPPER_22': 'O', 'READ_AVERAGE_22': 'O', 'MAT_AVERAGE_22': 'O', 'TELIG_19': 'O', 'PTFSM6CLA1A_19': 'O', 'PTNOTFSM6CLA1A_19': 'O', 'PTRWM_EXP_19': 'O', 'PTRWM_HIGH_19': 'O', 'PTRWM_EXP_FSM6CLA1A_19': 'O', 'PTRWM_HIGH_FSM6CLA1A_19': 'O', 'PTRWM_EXP_NotFSM6CLA1A_19': 'O', 'PTRWM_HIGH_NotFSM6CLA1A_19': 'O', 'READPROG_19': 'O', 'READPROG_LOWER_19': 'O', 'READPROG_UPPER_19': 'O', 'WRITPROG_19': 'O', 'WRITPROG_LOWER_19': 'O', 'WRITPROG_UPPER_19': 'O', 'MATPROG_19': 'O', 'MATPROG_LOWER_19': 'O', 'MATPROG_UPPER_19': 'O', 'READ_AVERAGE_19': 'O', 'MAT_AVERAGE_19': 'O', 'READPROG_UNADJUSTED': 'O', 'WRITPROG_UNADJUSTED': 'O', 'MATPROG_UNADJUSTED': 'O', 'READPROG_DESCR': 'O', 'WRITPROG_DESCR': 'O', 'MATPROG_DESCR': 'O'}
    
    logger.info("Loading Schools attainment file")
    # Load the attainment data
    df = pd.read_csv('data/attainment-2022-2023/england_ks2revised.csv', dtype=schema)

    logger.info("Cleaning up data")

    # Filter out RECTYPEs 3=Local Authority; 4=National (all schools); 5=National (state-funded schools only) 
    # as these are roll ups and don't all the data we need.
    df = df.loc[(df['RECTYPE'] != 3) & (df['RECTYPE'] != 4) & (df['RECTYPE'] != 5)]
    
    # Now that the roll ups are in these columns should cleanly convert to ints.
    df = df.astype({'ALPHAIND': int, 'LEA': int, 'ESTAB': int, 'URN': int})

    # Remove the NAs and convert to int. 
    df['URN_AC'] = pd.to_numeric(df['URN_AC'], errors='coerce')
    
    # Remove the duplicate columns create above.
    df = df.loc[:,~df.columns.duplicated()].copy()

    # For all columns greater than 23, convert them to numeric values, any that don't will be NaN.
    # this will null any data that is either not supplied or SUPP which seems to be suppressed.
    # Percents will be converted to floats. 
    for col in df.columns[23:]:
        cleanup_numeric(df, col)

    logger.info("Writing to parquet")
    df.to_parquet('data/duck/schools_attainment.parquet')

def import_academies_attainment():

    # Apply this schema
    schema = {'TIME_PERIOD': 'int64', 'TIME_IDENTIFIER': 'O', 'TRUST_GROUP_TYPE': 'O', 'TRUST_NAME': 'O', 'TRUST_UID': 'int64', 'TRUST_ID': 'O', 'TRUST_COMPANIES_HOUSE_NUMBER': 'int64', 'TRUST_UKPRN': 'int64', 'TRUST_LEADREGION': 'O', 'INSTITUTIONS_MATPTINC': 'O', 'NUMINST_MATPTINC': 'int64', 'NUMINST_FSM6CLA1A_MATPTINC': 'int64', 'NUMINST_CONVERTER_MATPTINC': 'int64', 'NUMINST_SPONSOR_MATPTINC': 'int64', 'NUMINST_FREE_MATPTINC': 'int64', 'NUMINST_3_MATPTINC': 'int64', 'NUMINST_4PLUS_MATPTINC': 'int64', 'TELIG_MATPTINC': 'int64', 'KS1APS_MATPTINC': 'float64', 'PFSM6CLA1A_MATPTINC': 'O', 'PNOTFSM6CLA1A_MATPTINC': 'O', 'PEALGRP2_MATPTINC': 'O', 'PSEN_MATPTINC': 'O', 'READ_PROGSCORE_EM_ADJ_WGTAVG': 'O', 'READ_PROGSCORE_ADJ_UPPER': 'O', 'READ_PROGSCORE_ADJ_LOWER': 'O', 'WRIT_PROGSCORE_EM_ADJ_WGTAVG': 'O', 'WRIT_PROGSCORE_ADJ_UPPER': 'O', 'WRIT_PROGSCORE_ADJ_LOWER': 'O', 'MAT_PROGSCORE_EM_ADJ_WGTAVG': 'O', 'MAT_PROGSCORE_ADJ_UPPER': 'O', 'MAT_PROGSCORE_ADJ_LOWER': 'O', 'READ_PROGSCORE_EM_ADJ_WGTAVG_FSM6CLA1A': 'O', 'READ_PROGSCORE_ADJ_UPPER_FSM6CLA1A': 'O', 'READ_PROGSCORE_ADJ_LOWER_FSM6CLA1A': 'O', 'WRIT_PROGSCORE_EM_ADJ_WGTAVG_FSM6CLA1A': 'O', 'WRIT_PROGSCORE_ADJ_UPPER_FSM6CLA1A': 'O', 'WRIT_PROGSCORE_ADJ_LOWER_FSM6CLA1A': 'O', 'MAT_PROGSCORE_EM_ADJ_WGTAVG_FSM6CLA1A': 'O', 'MAT_PROGSCORE_ADJ_UPPER_FSM6CLA1A': 'O', 'MAT_PROGSCORE_ADJ_LOWER_FSM6CLA1A': 'O', 'READ_PROGSCORE_EM_ADJ_WGTAVG_NOTFSM6CLA1A': 'O', 'READ_PROGSCORE_ADJ_UPPER_NOTFSM6CLA1A': 'O', 'READ_PROGSCORE_ADJ_LOWER_NOTFSM6CLA1A': 'O', 'WRIT_PROGSCORE_EM_ADJ_WGTAVG_NOTFSM6CLA1A': 'O', 'WRIT_PROGSCORE_ADJ_UPPER_NOTFSM6CLA1A': 'O', 'WRIT_PROGSCORE_ADJ_LOWER_NOTFSM6CLA1A': 'O', 'MAT_PROGSCORE_EM_ADJ_WGTAVG_NOTFSM6CLA1A': 'O', 'MAT_PROGSCORE_ADJ_UPPER_NOTFSM6CLA1A': 'O', 'MAT_PROGSCORE_ADJ_LOWER_NOTFSM6CLA1A': 'O', 'PRWM_EXP_WGTAVG': 'O', 'PRWM_EXP_WGTAVG_FSM6CLA1A': 'O', 'PRWM_EXP_WGTAVG_NOTFSM6CLA1A': 'O', 'READ_PROGSCORE_BANDING': 'O', 'WRIT_PROGSCORE_BANDING': 'O', 'MAT_PROGSCORE_BANDING': 'O', 'INSTITUTIONS_INMAT': 'O', 'NUMINST_INMAT': 'int64', 'NUMINST_CONVERTER_INMAT': 'int64', 'NUMINST_SPONSOR_INMAT': 'int64', 'NUMINST_FREE_INMAT': 'int64', 'TELIG_INMAT': 'int64', 'PFSM6CLA1A_INMAT': 'O', 'PNOTFSM6CLA1A_INMAT': 'O'}
    
    logger.info("Loading Academies attainment file")
    # Load the attainment data
    df = pd.read_csv('data/attainment-2022-2023/england_ks2-mats-performance.csv', dtype=schema, encoding='latin-1')

    logger.info("Cleaning up data") 

    # For all columns greater than 23, convert them to numeric values, any that don't will be NaN.
    # this will null any data that is either not supplied or SUPP which seems to be suppressed.
    # Percents will be converted to floats. 
    for col in df.columns[11:53]:
        cleanup_numeric(df, col)

    for col in df.columns[56:]:
        cleanup_numeric(df, col)

    logger.info("Writing to parquet")
    df.to_parquet('data/duck/academies_attainment.parquet')


def import_schools_attainment_labels():
    logger.info("Loading school attainment lables")
    df = pd.read_csv('data/attainment-2022-2023/ks2_meta.csv')
    df.info(verbose=True)
    logger.info("Writing to parquet")
    df.to_parquet('data/duck/schools_attainment_labels.parquet')

def import_academies_attainment_labels():
    logger.info("Loading school attainment lables")
    df = pd.read_csv('data/attainment-2022-2023/ks2-mats-performance_meta.csv')
    
    # Rename these columns to be consistent with the schools metadata
    df = df.rename(columns={"Metafile heading": "Field Name"})
    df = df.rename(columns={"Metafile description": "Label/Description"})
    
    df.drop(columns={"2019 field name","new for 2023"}, inplace=True)
    # remove dupes created. 
    df = df.loc[:,~df.columns.duplicated()].copy()

    df.info(verbose=True)
    logger.info("Writing to parquet")
    df.to_parquet('data/duck/academies_attainment_labels.parquet')
    
    
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logger.info('Starting up.')
    
    import_schools_attainment()
    import_schools_attainment_labels()
    
    import_academies_attainment()
    import_academies_attainment_labels()
    
    import_schools_finance()
    import_academies_finance()

