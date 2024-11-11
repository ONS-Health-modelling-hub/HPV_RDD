import pyspark.sql.functions as f
import pyspark.sql.functions as F
import os 
import de_utils

de_utils.rm_pre_wrap()


from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = (
    SparkSession.builder.appName("xl-session")
    .config("spark.executor.memory", "10g")
    .config("spark.yarn.executor.memoryOverhead", "2g")
    .config("spark.executor.cores", 5)
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.dynamicAllocation.maxExecutors", 20)
    .config("spark.sql.shuffle.partitions", 240)
    .config("spark.shuffle.service.enabled", "true")
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.sql.codegen.wholeStage", "false")
    .enableHiveSupport()
    .getOrCreate()
)

# load in hes
os.chdir(r'...')
sys.path.insert(0, r"...")

#read in functions
sys.path.insert(1, f'...')
import pysparkFunctions as pf


sys.path.insert(1, f'...')
import hes_processing_v3 as hes

# APC
apc = hes.loadHESAPC(
    spark,
    years=[
        "2009",
        "2010",
        "2011",
        "2012",
        "2013",
        "2014",
        "2015",
        "2016",
        "2017",
        "2018",
        "2019",
        "2020",
        "2021",
        "2022",
        "2023",
    ],
    make_cips_id=False,
    remove_episodes_with_blank_nhs_numbers=True,
)

# OP
op = hes.loadHESOP(
    spark,
    years=[
        "2009",
        "2010",
        "2011",
        "2012",
        "2013",
        "2014",
        "2015",
        "2016",
        "2017",
        "2018",
        "2019",
        "2020",
        "2021",
        "2022",
        "2023",
    ],
    remove_episodes_with_blank_nhs_numbers=True,
)


# read in lookup
import pandas as pd

lookup = (
    r".../lookups/cerv_canc_hes_lookup.csv"
)

lookup = pd.read_csv(lookup)

# return diag flags, inc. early_return to not agg
apc_2, op_2, _ = hes.personLevelNeverEverFlag(
    apc=apc, op=op, icdLookup=lookup, early_return=True
)


# Consistently name columns across tables - make sure you run this post flag
all_hes = hes.HESDeriveCrossSettingVariables(
    spark,
    apc=apc_2,
    op=op_2,
    newColumns={
        "start_date": {
            "apc": F.col("EPISTART"),
            "op": F.col("APPTDATE"),
            "ae": F.col("ARRIVALDATE"),
        },
        "setting": {"apc": F.lit("APC"), "op": F.lit("OP"), "ae": F.lit("AE")},
        "episode_identifier": {
            "apc": F.col("EPIKEY"),
            "op": F.col("ATTENDKEY"),
            "ae": F.col("AEKEY"),
        },
        # No end_date column for AE,so leave empty
        # Start and end date are same for OP
        "end_date": {
            "apc": F.col("EPIEND"),
            "op": F.col("APPTDATE"),
            "ae": F.lit(""),
        },
    },
    return_type="combined",
)

# drop
all_hes = all_hes.select(
    "NEWNHSNO",
    "start_date",
    "end_date",
    "episode_identifier",
    "CERVICAL_DYSPLASIA_flag",
    "CERVICAL_CANCER_flag",
)

# aggregate by person and take first episode recorded

all_hes = all_hes.withColumn(
    "CERVICAL_DYSPLASIA_flag_boo",
    f.when(f.col("CERVICAL_DYSPLASIA_flag") == "Ever", 1).otherwise(0),
)
all_hes = all_hes.withColumn(
    "CERVICAL_CANCER_flag_boo",
    f.when(f.col("CERVICAL_CANCER_flag") == "Ever", 1).otherwise(0),
)


#save out
pf.save_file_in_SQL(
    all_hes,
    None,
    "analytical_hpv_hes_variables",
    suffix=True,
    project="...",
)

