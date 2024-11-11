#script to prepare data tables for ONS data set release

library(dplyr)
library(sparklyr)
library(tidyverse)
library(lubridate)
library(stringr)
library(magrittr)
library(openxlsx)

#set up SPARK session 
config <- spark_config() 
config$spark.dynamicAllocation.maxExecutors <- 30
config$spark.executor.cores <- 5
config$spark.executor.memory <- "20g"
config$spark.driver.maxResultSize <- "10g"
sc <- spark_connect(master = "yarn-client",
                    app_name = "HPV_data",
                    config = config,
                    version = "2.3.0")

#user inputs
path <- ".../HPV/"

source(paste0(path, "_Functions.R"))

#input running date 
cohort_non <- "_non_cohort_vs_catch_up"
cohort_routine <- "_routine_vs_catch_up"

#sdc function -b round all to nearest 5 

#read in person level data set
df_analysis_non_vs_catchup <- sdf_sql(sc, paste0("SELECT * FROM ....", "analytical_hpv_[date]_non_cohort_vs_catch_up_age_23_30")) %>% 
 mutate(cohort = case_when(dob_census <= "1991-08-31" & dob_census >= "1990-09-01" ~ "Cohort 2", 
                           dob_census <= "1992-08-31" & dob_census >= "1991-09-01" ~ "Cohort 3", 
                           dob_census <= "1990-08-31" & dob_census >= "1989-09-01" ~ "Non-Cohort A", 
                           dob_census <= "1989-08-31" & dob_census >= "1988-09-01" ~ "Non-Cohort B", 
                           dob_census <= "1988-08-31" & dob_census >= "1987-09-01" ~ "Non-Cohort C"),
       analysis = "Non-vaccinated versus catch-up vaccination")

df_analysis_catchup_vs_routine <- sdf_sql(sc, paste0("SELECT * FROM ....", "analytical_hpv_[date]_routine_vs_catch_up_age_19_25")) %>% 
  mutate(cohort = case_when(dob_census <= "1993-08-31" & dob_census >= "1992-09-01" ~ "Cohort 4", 
                           dob_census <= "1994-08-31" & dob_census >= "1993-09-01" ~ "Cohort 5", 
                           dob_census <= "1995-08-31" & dob_census >= "1994-09-01" ~ "Cohort 6", 
                           dob_census <= "1996-08-31" & dob_census >= "1995-09-01" ~ "Cohort 1", 
                           dob_census <= "1997-08-31" & dob_census >= "1996-09-01" ~ "Cohort 7", 
                           dob_census <= "1998-08-31" & dob_census >= "1997-09-01" ~ "Cohort 8"),
       analysis = "Catch-up versus routine vaccination")


df_analysis <- rbind(df_analysis_non_vs_catchup, df_analysis_catchup_vs_routine) %>% 
  mutate(vax_group = case_when(cohort == "Cohort 2" ~ "Catch-up vaccination",
                               cohort == "Cohort 3" ~ "Catch-up vaccination", 
                               cohort == "Non-Cohort A" ~ "Non-vaccinated", 
                               cohort == "Non-Cohort B" ~ "Non-vaccinated", 
                               cohort == "Non-Cohort C" ~ "Non-vaccinated",
                               cohort == "Cohort 4" ~ "Catch-up vaccination", 
                               cohort == "Cohort 5" ~ "Catch-up vaccination", 
                               cohort == "Cohort 6" ~ "Catch-up vaccination", 
                               cohort == "Cohort 1" ~ "Routine vaccination", 
                               cohort == "Cohort 7" ~ "Routine vaccination", 
                               cohort == "Cohort 8" ~ "Routine vaccination"))

###Table 1: Cohorts #######################################################

table1 <- df_analysis %>%
  group_by(cohort, vax_group, analysis) %>% 
  count() 

table1 <- table1 %>% arrange(analysis, vax_group) %>% collect() %>%
  select(analysis, cohort, vax_group, n) %>%
  rename(Analysis = analysis, 
         Cohort = cohort, 
         Group = vax_group, 
         'Total (n)' = n)

table1 %>% print()

##Table 2: Population characteristics #######################################################

df_analysis %<>%
  mutate(ethnicity_short =
      case_when(ethpuk11_census == '01'~  'White',
                ethpuk11_census == '02'~  'White',
                ethpuk11_census == '03'~  'White',
                ethpuk11_census == '04'~  'White',
                ethpuk11_census == '05'~  'Mixed',
                ethpuk11_census == '06'~  'Mixed',
                ethpuk11_census == '07'~  'Mixed',
                ethpuk11_census == '08'~  'Mixed',
                ethpuk11_census == '09'~  'Asian',
                ethpuk11_census == '10'~  'Asian',
                ethpuk11_census == '11'~  'Asian',
                ethpuk11_census == '12'~  'Asian',
                ethpuk11_census == '13'~  'Asian',
                ethpuk11_census == '14'~  'Black',
                ethpuk11_census == '15'~  'Black',
                ethpuk11_census == '16'~  'Black',
                ethpuk11_census == '17'~  'Other ethnic group',
                ethpuk11_census == '18'~  'Other ethnic group',
                ethpuk11_census == 'XX'~  'No code required',
                TRUE ~ 'missing'))

group_eth <- df_analysis %>% 
  group_by(vax_group, analysis, ethnicity_short) %>%  
  summarise(Total = n()) %>%
  mutate_if(is.numeric, round, 2) %>% 
  mutate(Characteristic = "Ethnicity") %>% 
  rename(Level = ethnicity_short) %>%
  collect() %>%
  arrange(vax_group, analysis, Level)

group_eth %>% print()

group_imd <- df_analysis %>% 
  group_by(vax_group, analysis, wimd_imd_quintile) %>%  
  summarise(Total = n()) %>%
  mutate_if(is.numeric, round, 2) %>% 
  mutate(Characteristic = "Index of Multiple Deprivation") %>% 
  rename(Level = wimd_imd_quintile) %>%
  collect() %>%
  arrange(vax_group, analysis, Level)

group_imd %>% print()

table2 <- rbind(group_eth, group_imd) %>%
  select(analysis, vax_group, Characteristic, Level, Total) %>% 
  rename(Analysis = analysis, 
         Group = vax_group, 
         'Total (n)' = Total)

table2 %>% print()

##Table 3: Rates of outcomes by cohort ####################################

#create flag for 'ANY' cervical outcome
## joint outcomes - rates of both
df_analysis %<>%
  mutate(cerv_outcome_any = ifelse(cerv_can == 1 | cerv_dysp == 1, 1, 0))

grouped_df <- df_analysis %>% 
  group_by(vax_group, analysis) %>%
  summarise(total_population = n(),
        total_cerv_can_death = sum(cerv_canc_death),
        total_anal_can_death = sum(anal_canc_death),
        total_cerv_can = sum(cerv_can),
        total_cerv_dysp = sum(cerv_dysp),
        total_any_cerv_outcome  = sum(cerv_outcome_any),
				total_any_othernoncan_death = sum(any_other_noncanc_death)) %>%
    collect()

grouped_df <- grouped_df %>%
  mutate(cancer_diagnosis_rate = total_cerv_can/total_population*100000,
        dysplasia_diagnosis_rate = total_cerv_dysp/total_population*100000,
        cancer_mortality_rate = total_cerv_can_death/total_population*100000,
        anal_diagnosis_rate = total_anal_can_death/total_population*100000,
        any_cerv_diagnosis_rate = total_any_cerv_outcome/total_population*100000,
				any_othernoncan_mortality_rate = total_any_othernoncan_death/total_population*100000)
 
table3 <- grouped_df %>%
  select(analysis, vax_group, total_population, total_cerv_dysp, 
         total_cerv_can, total_cerv_can_death, total_any_othernoncan_death,
         dysplasia_diagnosis_rate, cancer_diagnosis_rate, cancer_mortality_rate, 
         any_othernoncan_mortality_rate) %>% 
  rename(Analysis = analysis, 
         Group = vax_group, 
         'Total (n)' = total_population, 
         'Cervical dysplasia diagnosis (total)' = total_cerv_dysp, 
         'Cervical cancer diagnosis (total)' = total_cerv_can,
         'Cervical cancer mortality (total)' = total_cerv_can_death,
         'Any other non-cancer mortality (total)' = total_any_othernoncan_death,
         'Cervical dysplasia diagnosis (rate per 100,000)' = dysplasia_diagnosis_rate, 
         'Cervical cancer diagnosis (rate per 100,000)' = cancer_diagnosis_rate,
         'Cervical cancer mortality (rate per 100,000)' = cancer_mortality_rate,
         'Any other non-cancer mortality (rate per 100,000)' = any_othernoncan_mortality_rate) %>%
  mutate_if(is.numeric, ~round(., 2)) %>% 
  arrange(Analysis, Group)

##keep counts of events columsn 
table3 %>% print()

##Table 4: Rates by brith month (equivalent to the main figures) ####################################
max_year_non_cu <- 1987
max_year_routine_cu <- 1992

#create running variable depending on cohort 
df_analysis %<>%
  mutate(dob_month = ifelse(analysis == "Non-vaccinated versus catch-up vaccination", 
                            dob_month_census + (dob_year_census - max_year_non_cu)*12 -9, 
                            dob_month_census + (dob_year_census - max_year_routine_cu)*12 -9)) 

#calcualte rates in long format by birth month for cohort
grouped_df_month <- df_analysis %>% 
  group_by(dob_month, analysis) %>%
  summarise(total_population = n(),
        total_cerv_can_death = sum(cerv_canc_death),
        total_cerv_can = sum(cerv_can),
        total_cerv_dysp = sum(cerv_dysp),
				total_any_othernoncan_death = sum(any_other_noncanc_death)) %>%
    collect()

grouped_df_month <- grouped_df_month %>%
  mutate(cancer_diagnosis_rate = total_cerv_can/total_population*100000,
        dysplasia_diagnosis_rate = total_cerv_dysp/total_population*100000,
        cancer_mortality_rate = total_cerv_can_death/total_population*100000,
				any_othernoncan_mortality_rate = total_any_othernoncan_death/total_population*100000) 
  
table4 <- grouped_df_month %>%
  mutate(time = "Month") %>%
  select(analysis, time, dob_month, total_population, dysplasia_diagnosis_rate,
        cancer_diagnosis_rate, cancer_mortality_rate, 
        any_othernoncan_mortality_rate) %>% 
  arrange(analysis, dob_month) %>%
  rename(Analysis = analysis, 
        'Running variable' = time,
         'Interval' = dob_month, 
         'Total (n)' = total_population, 
         'Cervical dysplasia diagnosis (rate per 100,000)' = dysplasia_diagnosis_rate, 
         'Cervical cancer diagnosis (rate per 100,000)' = cancer_diagnosis_rate,
         'Cervical cancer mortality (rate per 100,000)' = cancer_mortality_rate,
         'Any other non-cancer mortality (rate per 100,000)' = any_othernoncan_mortality_rate) %>%
  mutate_if(is.numeric, ~round(., 2)) 

table4 %>% print()

##JUST FOR NON-VS-CATCHUP FROM HERE ONWARDS
##Table 5: VE estimates ########################################################################

## read in the wide versions and tidy
non_cu_rdd <- read_csv(paste0(path, "outputs/[date]_non_cohort_vs_catch_up/non_cohort_vs_catch_up_rdd_store.csv"))

table5 <- non_cu_rdd[,c(1,3,5,7)] %>% 
  rename(name = "...1") %>% 
  mutate(Value = case_when(name == "Coef" ~ "LATE/100,000", 
                           name == "CI_lower" ~ " LATE/100,000: 95% confidence interval (lower bound)",
                           name == "CI_upper" ~ "LATE/100,000: 95% confidence interval (upper bound)",
                           name == "p_value" ~ "P-Value",
                           name == "se" ~ "Standard error",
                           name == "sharp_disc" ~ "Discontinuity",
                           name == "CI_lower_disc" ~ "Discontinuity: 95% confidence interval (lower bound)",
                           name == "CI_upper_disc" ~ "Discontinuity: 95% confidence interval (upper bound)", 
                           name == "ve" ~ "Vaccine effectiveness",
                           name == "ve_lower" ~ "Vaccine effectiveness: 95% confidence interval (lower bound)",
                           name == "ve_upper" ~ "Vaccine effectiveness: 95% confidence interval (upper bound)")) %>% 
  select(Value, 
        dysplasia_diagnosis_rate_month,
        cancer_diagnosis_rate_month,
        any_othernoncan_mortality_rate_month) %>% 
  filter(!is.na(Value)) %>% 
  collect() 

table5 <- table5 %>%
   rename('Cervical dysplasia diagnosis (rate per 100,000)' = dysplasia_diagnosis_rate_month, 
         'Cervical cancer diagnosis (rate per 100,000)' = cancer_diagnosis_rate_month,
         'Any other non-cancer mortality (rate per 100,000)' = any_othernoncan_mortality_rate_month) %>%
  mutate_if(is.numeric, ~round(., 2)) 

table5 %>% print()

## Table 6 (supplement): Proportion of cohort with certain characteristics


#IMD
percentage_imd <- df_analysis %>%
  group_by(analysis, cohort, dob_month, wimd_imd_quintile) %>%
  count() %>%
  collect()

imd <- percentage_imd %>%
  group_by(analysis, cohort, dob_month) %>%
  mutate(total = sum(n)) %>%
  mutate(percentage_imd = n/total*100) %>% 
  arrange(cohort)

imd_group <- imd %>% 
  mutate(Variable = "IMD", 
         'Running variable' = "Month") %>% 
  rename(Analysis = analysis, 
         Cohort = cohort,
         Interval = dob_month, 
         Group = wimd_imd_quintile,
        'Total (n)' = n, 
         Percentage = percentage_imd) %>% 
  select(Analysis, Cohort, Variable, Group, 'Running variable', Interval, 'Total (n)', 'Percentage') %>% 
  arrange(Interval, Cohort) 

imd_group$Group <- as.character(imd_group$Group)

#Ethnicity 
percentage_eth <- df_analysis %>%
  group_by(analysis, cohort, dob_month, ethnicity_short) %>%
  count() %>%
  collect()

eth <- percentage_eth %>%
  group_by(analysis, cohort, dob_month) %>%
  mutate(total = sum(n)) %>%
  mutate(percentage_eth = n/total*100) %>% 
  arrange(cohort)

eth_group <- eth %>% 
  mutate(Variable = "Ethnicity", 
         'Running variable' = "Month") %>% 
  rename(Analysis = analysis, 
         Cohort = cohort,
         Interval = dob_month, 
         Group = ethnicity_short,
        'Total (n)' = n, 
        Percentage = percentage_eth)  %>% 
  select(Analysis, Cohort, Variable, Group, 'Running variable', Interval, 'Total (n)', 'Percentage') %>% 
  arrange(Interval, Cohort)

table6 <- rbind(imd_group, eth_group)

table6 %>% print()

##Table 7 (supplement) RDD with different cut-offs: 

## read in the wide versions and tidy
non_cu_cutoff <- read_csv(paste0(path, "outputs/[date]_non_cohort_vs_catch_up/non_cohort_vs_catch_up_cuttoff_sensitivity.csv"))

table7 <- non_cu_cutoff %>%
  rename(name = "...1") %>% 
  mutate(Value = case_when(name == "Coef" ~ "LATE/100,000", 
                           name == "CI_lower" ~ " LATE/100,000: 95% confidence interval (lower bound)",
                           name == "CI_upper" ~ "LATE/100,000: 95% confidence interval (upper bound)",
                           name == "p_value" ~ "P-Value")) %>% 
  rename('Outcome: Cervical Dysplasia, Cut-off: 34' = '34_dysplasia_diagnosis_rate_dysplasia',
        'Outcome: Cervical Dysplasia, Cut-off: 35' = '35_dysplasia_diagnosis_rate_dysplasia',
        'Outcome: Cervical Dysplasia, Cut-off: 36' = '36_dysplasia_diagnosis_rate_dysplasia',
        'Outcome: Cervical Dysplasia, Cut-off: 37' = '37_dysplasia_diagnosis_rate_dysplasia',
        'Outcome: Cervical Dysplasia, Cut-off: 38' = '38_dysplasia_diagnosis_rate_dysplasia',
        'Outcome: Cervical Cancer, Cut-off: 34' = '34_cancer_diagnosis_rate_cervcancer',
        'Outcome: Cervical Cancer, Cut-off: 35' = '35_cancer_diagnosis_rate_cervcancer',
        'Outcome: Cervical Cancer, Cut-off: 36' = '36_cancer_diagnosis_rate_cervcancer',
        'Outcome: Cervical Cancer, Cut-off: 37' = '37_cancer_diagnosis_rate_cervcancer',
        'Outcome: Cervical Cancer, Cut-off: 38' = '38_cancer_diagnosis_rate_cervcancer') %>% 
  select('Value', 'Outcome: Cervical Dysplasia, Cut-off: 34', 'Outcome: Cervical Dysplasia, Cut-off: 35',
        'Outcome: Cervical Dysplasia, Cut-off: 36', 'Outcome: Cervical Dysplasia, Cut-off: 37',
        'Outcome: Cervical Dysplasia, Cut-off: 38', 'Outcome: Cervical Cancer, Cut-off: 34',
        'Outcome: Cervical Cancer, Cut-off: 35', 'Outcome: Cervical Cancer, Cut-off: 36',
        'Outcome: Cervical Cancer, Cut-off: 37', 'Outcome: Cervical Cancer, Cut-off: 38') %>%
  mutate_if(is.numeric, ~round(., 2)) 

table7 %>% print()

##save all outputs #########################################################################

dataset_names <- list('Table 1' = table1, 
                     'Table 2' = table2, 
                     'Table 3' = table3,
                     'Table 4' = table4,
                     'Table 5' = table5, 
                     'Table 6 (supplement)' = table6, 
                     'Table 7 (supplement)' = table7)

write.xlsx(dataset_names, paste0(path,"outputs/ons_data_tables_", Sys.Date(), ".xlsx"))

