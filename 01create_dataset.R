# create analytical dataset for HPV analysis
# study population women in Census 2011, resident in England
# usual resident of UK
# variables: dob, dod, cervical cancer cause of death flag, diagnosis outcome flags, sociodemographic variables 
# dob months


library(dplyr)
library(sparklyr)
library(tidyverse)
library(lubridate)
library(stringr)


#--------------------------
# Set up the spark connection
#--------------------------

config <- spark_config() 
config$spark.dynamicAllocation.maxExecutors <- 30
config$spark.executor.cores <- 5
config$spark.executor.memory <- "20g"
config$spark.driver.maxResultSize <- "10g"
sc <- spark_connect(master = "yarn-client",
                    app_name = "HPV_data",
                    config = config,
                    version = "2.3.0")

#--------------------------
# User inputs
#--------------------------

phda_version <- "20240113" 

hes_diagnosis_version <- "20240402"

#specify comparrison - user input
#type <- "routine_vs_catch_up" 
type <- "non_cohort_vs_catch_up"


if (type == "routine_vs_catch_up"){
  age_start_population <- 19

  age_start_followup <- 19

  age_end_followup <- 25
  
  dob_min <- "1992-09-01" # cohort 4
  dob_max <- "1997-08-31" # cohort 7
}else if (type == "non_cohort_vs_catch_up"){
  ### cohort vs non
  age_start_population <- 23

  age_start_followup <- 23

  age_end_followup <- 30
  
  dob_min <- "1987-09-01" # cohort Non-C
  dob_max <- "1992-08-31" # cohort 3
}

#--------------------------
# Read in PHDA dataset and select study population
#--------------------------
# read in census 2011
df_phda <- sdf_sql(sc, paste0("SELECT * FROM ...", phda_version))

df_phda %>% summarise(min(dor_deaths), min(dod_deaths), max(dor_deaths), max(dod_deaths)) %>% collect()

# select and variables needed
df_phda <- df_phda %>%
  select(dob_census, dod_deaths, fic10und_deaths, sex_census, region_code_census, uresindpuk11_census,
        dob_year_census, dob_month_census, dob_day_census, ageinyrs_deaths, final_nhs_number,
        fic10men1_deaths, fic10men2_deaths, fic10men3_deaths, fic10men4_deaths, fic10men5_deaths,
        fic10men6_deaths, fic10men7_deaths, fic10men8_deaths, fic10men9_deaths, fic10men10_deaths,
        fic10men11_deaths, fic10men12_deaths, fic10men13_deaths, fic10men14_deaths, fic10men15_deaths,
        cen_pr_flag, ethpuk11_census, wimd_imd_decile, wimd_imd_quintile)

# filter to enumerated in census and usual resident of UK 
df_phda <- df_phda %>%
  filter(uresindpuk11_census == "1" & cen_pr_flag == "1")

# filter to resident in England 
df_phda <- df_phda %>%
  mutate(country = substr(region_code_census, 1, 1)) %>%
  filter(country == "E")

# filter to female at census 2011
df_phda <- df_phda %>%
  filter(sex_census =="2")

# filter to birth date 
df_phda <- df_phda %>%
  mutate(dob_census = date(timestamp(dob_census))) %>%
  filter(dob_census >= dob_min & dob_census <= dob_max)

# filter to alive at start of follow-up
df_phda <- df_phda %>%
  filter(is.na(dod_deaths) | (!is.na(dod_deaths) & ageinyrs_deaths >= age_start_population))

#--------------------------
# Create HES outcome flags
#--------------------------

df_hes <- sdf_sql(sc, paste0("SELECT * FROM ...", hes_diagnosis_version))

# join on comorbidities (episode level)
df_hes <- left_join(df_hes, select(df_phda, final_nhs_number, dob_census), by = c("NEWNHSNO" = "final_nhs_number"))

## calculate age on diagnosis using dob for the people in the dataset
df_hes <- df_hes %>%
  mutate(age_at_dysplasia_diag = datediff(start_date, dob_census)/365.25) %>%
  mutate(age_at_cancer_diag = datediff(start_date, dob_census)/365.25)

#create flags for diagnosis in follow up period
df_hes <- df_hes %>%
  mutate(cerv_can_flag = ifelse(CERVICAL_CANCER_flag_boo==1 & age_at_cancer_diag >= age_start_followup & age_at_cancer_diag < age_end_followup, 1, 0),
        cerv_dysp_flag = ifelse(CERVICAL_DYSPLASIA_flag_boo==1 & age_at_dysplasia_diag >= age_start_followup & age_at_dysplasia_diag < age_end_followup, 1, 0))

df_hes %>% summarise(min(age_at_dysplasia_diag), min(age_at_cancer_diag), max(age_at_dysplasia_diag), max(age_at_cancer_diag))

#de-duplicate if multiple diagnoses
df_hes <- df_hes %>%
  group_by(NEWNHSNO) %>%
  mutate(cerv_can = ifelse(sum(cerv_can_flag, na.rm=TRUE)>0, 1, 0),
         cerv_dysp = ifelse(sum(cerv_dysp_flag, na.rm=TRUE)>0, 1, 0)) %>%
  ungroup() %>%
  select(NEWNHSNO, cerv_can, cerv_dysp) %>%
  distinct()

#--------------------------
# Link on HES outcome flags
#--------------------------

df_linked <- left_join(df_phda, df_hes, by = c("final_nhs_number" = "NEWNHSNO"))

# set NA to 0
# replace na
df_linked <- df_linked %>%
 replace_na(list(cerv_can=0, cerv_dysp=0)) 

#--------------------------
# Create outcome and exposure variables
#--------------------------

can_lookup <- copy_to(sc,read.csv(paste0('.../lookups/allcancer_lookup.csv')),overwrite=TRUE)

df_linked <- df_linked %>%
  mutate(death = ifelse(!is.na(dod_deaths), 1, 0)) %>%
  mutate(underlying_code = str_sub(fic10und_deaths, 1, 3)) %>%
  left_join(can_lookup, by = c("underlying_code" = "Code"), copy = T) %>% 
  mutate(cause_death = case_when(death == 1 & Name == "cancer" ~ "Death: Cancer", 
                                 death == 1 & is.na(Name) ~ "Death: Other", 
                                 death == 0 ~ "Alive"))

# cervical cancer death  
df_linked <- df_linked %>%
  mutate(cerv_canc_death = ifelse((!is.na(fic10und_deaths) & substr(fic10und_deaths, 1, 3) == "C53") |
                                  (!is.na(fic10men1_deaths) & substr(fic10men1_deaths, 1, 3) == "C53") |
                                  (!is.na(fic10men2_deaths) & substr(fic10men2_deaths, 1, 3) == "C53") |
                                  (!is.na(fic10men3_deaths) & substr(fic10men3_deaths, 1, 3) == "C53") |
                                  (!is.na(fic10men4_deaths) & substr(fic10men4_deaths, 1, 3) == "C53") |
                                  (!is.na(fic10men5_deaths) & substr(fic10men5_deaths, 1, 3) == "C53") |
                                  (!is.na(fic10men6_deaths) & substr(fic10men6_deaths, 1, 3) == "C53") |
                                  (!is.na(fic10men7_deaths) & substr(fic10men7_deaths, 1, 3) == "C53") |
                                  (!is.na(fic10men8_deaths) & substr(fic10men8_deaths, 1, 3) == "C53") |
                                  (!is.na(fic10men9_deaths) & substr(fic10men9_deaths, 1, 3) == "C53") |
                                  (!is.na(fic10men10_deaths) & substr(fic10men10_deaths, 1, 3) == "C53") |
                                  (!is.na(fic10men11_deaths) & substr(fic10men11_deaths, 1, 3) == "C53") |
                                  (!is.na(fic10men12_deaths) & substr(fic10men12_deaths, 1, 3) == "C53") |
                                  (!is.na(fic10men13_deaths) & substr(fic10men13_deaths, 1, 3) == "C53") |
                                  (!is.na(fic10men14_deaths) & substr(fic10men14_deaths, 1, 3) == "C53") |
                                  (!is.na(fic10men15_deaths) & substr(fic10men15_deaths, 1, 3) == "C53"), 1, 0),
         anal_canc_death = ifelse((!is.na(fic10und_deaths) & substr(fic10und_deaths, 1, 3) == "C21") |
                                  (!is.na(fic10men1_deaths) & substr(fic10men1_deaths, 1, 3) == "C21") |
                                  (!is.na(fic10men2_deaths) & substr(fic10men2_deaths, 1, 3) == "C21") |
                                  (!is.na(fic10men3_deaths) & substr(fic10men3_deaths, 1, 3) == "C21") |
                                  (!is.na(fic10men4_deaths) & substr(fic10men4_deaths, 1, 3) == "C21") |
                                  (!is.na(fic10men5_deaths) & substr(fic10men5_deaths, 1, 3) == "C21") |
                                  (!is.na(fic10men6_deaths) & substr(fic10men6_deaths, 1, 3) == "C21") |
                                  (!is.na(fic10men7_deaths) & substr(fic10men7_deaths, 1, 3) == "C21") |
                                  (!is.na(fic10men8_deaths) & substr(fic10men8_deaths, 1, 3) == "C21") |
                                  (!is.na(fic10men9_deaths) & substr(fic10men9_deaths, 1, 3) == "C21") |
                                  (!is.na(fic10men10_deaths) & substr(fic10men10_deaths, 1, 3) == "C21") |
                                  (!is.na(fic10men11_deaths) & substr(fic10men11_deaths, 1, 3) == "C21") |
                                  (!is.na(fic10men12_deaths) & substr(fic10men12_deaths, 1, 3) == "C21") |
                                  (!is.na(fic10men13_deaths) & substr(fic10men13_deaths, 1, 3) == "C21") |
                                  (!is.na(fic10men14_deaths) & substr(fic10men14_deaths, 1, 3) == "C21") |
                                  (!is.na(fic10men15_deaths) & substr(fic10men15_deaths, 1, 3) == "C21"), 1, 0),
          age_at_death = datediff(dod_deaths, dob_census)/365.25) %>%
  mutate(cerv_canc_death = ifelse(cerv_canc_death==1 & age_at_death >= age_start_followup & age_at_death < age_end_followup, 1, 0),
				 any_other_noncanc_death = ifelse(death == 1 & cause_death == "Death: Other" & 
																							age_at_death >= age_start_followup & age_at_death < age_end_followup, 1, 0))
         
#--------------------------
# Save out person level dataset to hue
#--------------------------

date_of_run <- str_remove_all(Sys.Date(), "-")

table_name = paste0('cen_dth_hes.analytical_hpv_', date_of_run, "_", type, "_age_", age_start_followup, "_", age_end_followup)


## Delete table
sql <- paste0('DROP TABLE IF EXISTS ', table_name)
print(sql)
DBI::dbExecute(sc, sql)

## save
sdf_register(df_linked , 'df_linked')
sql <- paste0('CREATE TABLE ', table_name, ' AS SELECT * FROM df_linked')
DBI::dbExecute(sc, sql)