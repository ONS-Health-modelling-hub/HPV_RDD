library(dplyr)
library(sparklyr)
library(tidyverse)
library(lubridate)
library(stringr)
library(magrittr)
library(rdrobust)

#--------------------------
# Set up
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


source(".../_Functions.R")


#--------------------------
# User inputs
#--------------------------

##SPECIFY VERSION 
#name <- "analytical_hpv_20240403_non_cohort_vs_catch_up_age_23_30"
name <- "analytical_hpv_20240403_routine_vs_catch_up_age_19_25"

#specification of follow-up times depending on output
if (grepl("non", name)){
	
	age_start_followup <- 23

  age_end_followup <- 30
  
  X <- 46 #end of substr value to be used in saving out group vars
	
	cohort <- "non_cohort_vs_catch_up"
  
  outcome_list <- c("dysplasia_diagnosis_rate", 
								  "cancer_diagnosis_rate",
                  "any_othernoncan_mortality_rate")
  

}else if (grepl("routine", name)){
	age_start_followup <- 19

  age_end_followup <- 26
  
  X <- 43
	
	cohort <- "routine_vs_catch_up"
  
}

path <- ".../HPV/"

# folder to save results
date_of_run <- str_remove_all(Sys.Date(), "-")

dir.create(paste0(path, "outputs/", date_of_run, "_", cohort, "/"))
outputs_folder <- paste0(path, "outputs/", date_of_run, "_", cohort, "/")

directories <- c("exploratory_plots_of_rates", "discontinuity_fits_and_plots",
                "discontinuity_fits_and_plots/cancer_diag",
                "discontinuity_fits_and_plots/cancer_mortality", 
                "discontinuity_fits_and_plots/dysplasia_diag",
                "discontinuity_fits_and_plots/all_non_canc_mortality")

for (i in directories){
  dir.create(paste0(outputs_folder, i))

}


#--------------------------
# Read in person level dataset
#--------------------------

# read in data - change hue table to hes
df_analysis <- sdf_sql(sc, paste0("SELECT * FROM ...", name))


if (grepl("non", name)){

#cohorts 2 and 3 are catch-up, cohorts A, B, C are non
df_analysis %<>% 
  mutate(cohort = case_when(dob_census <= "1991-08-31" & dob_census >= "1990-09-01" ~ "Cohort 2", 
                           dob_census <= "1992-08-31" & dob_census >= "1991-09-01" ~ "Cohort 3", 
                           dob_census <= "1990-08-31" & dob_census >= "1989-09-01" ~ "Non-Cohort A", 
                           dob_census <= "1989-08-31" & dob_census >= "1988-09-01" ~ "Non-Cohort B", 
                           dob_census <= "1988-08-31" & dob_census >= "1987-09-01" ~ "Non-Cohort C"))

max_year <- 1987
	
} else if (grepl("routine", name)){

#cohorts 4, 5, 6 are catch-up, cohorts 1, 7, 8 are routine
df_analysis %<>% 
  mutate(cohort = case_when(dob_census <= "1993-08-31" & dob_census >= "1992-09-01" ~ "Cohort 4", 
                           dob_census <= "1994-08-31" & dob_census >= "1993-09-01" ~ "Cohort 5", 
                           dob_census <= "1995-08-31" & dob_census >= "1994-09-01" ~ "Cohort 6", 
                           dob_census <= "1996-08-31" & dob_census >= "1995-09-01" ~ "Cohort 1", 
                           dob_census <= "1997-08-31" & dob_census >= "1996-09-01" ~ "Cohort 7", 
                           dob_census <= "1998-08-31" & dob_census >= "1997-09-01" ~ "Cohort 8"))

max_year <- 1992

}

#create running variables
df_analysis %<>%
  mutate(dob_month_hpv = dob_month_census + (dob_year_census - max_year)*12 -9) %>%
  mutate(dob_academic_year_hpv = floor((dob_month_hpv)/12),
         dob_quarter_hpv = floor(dob_month_hpv/3))


#--------------------------
# Rate calculations and plots
#--------------------------

#discontinuty values
if (grepl("non", name)){
	
discont_quater <- 11.5
discont_month <- 36
	
} else if (grepl("routine", name)){

discont_quater <- 11.5
discont_month <- 36
	
}


colnames(df_analysis) 
#df_analysis %>% group_by(cerv_canc_death) %>% count()

# by quarter
grouped_df_quarter <- compute_rates("dob_quarter_hpv", df_analysis)

data.frame(grouped_df_quarter[order(grouped_df_quarter$dob_quarter_hpv),])

grouped_df_quarter_long <- grouped_df_quarter %>%
  pivot_longer(cols=c('cancer_diagnosis_rate', 'dysplasia_diagnosis_rate',
                      'cancer_mortality_rate', 'any_othernoncan_mortality_rate',
											'total_population'), 
               names_to = 'outcome', 
               values_to='rate')


ggplot(data = grouped_df_quarter_long, aes(x=dob_quarter_hpv, y=rate)) +
  geom_point() +
  facet_wrap(~outcome, scales = "free_y") +
  geom_vline(xintercept = discont_quater) +
  labs(x = paste0("Birth quarter ", cohort), y = "Mortality/diagnosis rate ages ", 
			 age_start_followup, "-", age_end_followup, "per 100,000")


#how do we want our data structured for outputs?
ggsave(paste0(outputs_folder, "exploratory_plots_of_rates/", "outcomes_", 
              age_start_followup, "_to_", age_end_followup, "_", cohort, "_by_birth_quarter.png"))

##save the outputs for plots: 

grouped_df_quarter_store <- grouped_df_quarter %>% 
  mutate_if(is.numeric, round, 2) %>%  
  arrange(dob_quarter_hpv) %>%
  rename("DOB Quarter" = dob_quarter_hpv,
        "Total population" = total_population,
        "Cervical cancer deaths (total)" = total_cerv_can_death,
        "Cervical cancer diagnosis (total)" = total_cerv_can,
        "Cervical dysplasia diagnosis (total)" = total_cerv_dysp,
        "Any other cause death (total)" = total_any_othernoncan_death,
        "Cervical cancer deaths (rate, per 100,000)" = cancer_diagnosis_rate,
        "Cervical cancer diagnosis (rate, per 100,000)" = cancer_mortality_rate,
        "Cervical dysplasia diagnosis (rate, per 100,000)" = dysplasia_diagnosis_rate,
        "Any other cause death (rate, per 100,000)" = any_othernoncan_mortality_rate)

grouped_df_quarter_store %>% print()

write.csv(grouped_df_quarter_store, paste0(path, "outputs/", 
                                           date_of_run, "_", 
                                           cohort, "/rates_by_quarter.csv"))


# by month
grouped_df_month <- compute_rates("dob_month_hpv", df_analysis)

data.frame(grouped_df_month[order(grouped_df_month$dob_month_hpv),])

grouped_df_month_long <- grouped_df_month %>%
  pivot_longer(cols=c('cancer_diagnosis_rate', 'dysplasia_diagnosis_rate',
                      'cancer_mortality_rate', 'any_othernoncan_mortality_rate',
											'total_population'), 
               names_to = 'outcome', 
               values_to='rate')

ggplot(data = grouped_df_month_long, aes(x=dob_month_hpv, y=rate)) +
  geom_point() +
  facet_wrap(~outcome, scales = "free_y") +
  geom_vline(xintercept=36) +
  labs(x = paste0("Birth month ", cohort), y = "Mortality/diagnosis rate ages ",
 							age_start_followup, "-", age_end_followup, "per 100,000")


ggsave(paste0(outputs_folder, "exploratory_plots_of_rates/", "outcomes_", 
              age_start_followup, "_to_", age_end_followup, "_", cohort, "_by_birth_month.png"))


grouped_df_month_store <- grouped_df_month %>% 
  mutate_if(is.numeric, round, 2) %>%  
  arrange(dob_month_hpv) %>%
  rename("DOB Month" = dob_month_hpv,
        "Total population" = total_population,
        "Cervical cancer deaths (total)" = total_cerv_can_death,
        "Cervical cancer diagnosis (total)" = total_cerv_can,
        "Cervical dysplasia diagnosis (total)" = total_cerv_dysp,
        "Any other cause death (total)" = total_any_othernoncan_death,
        "Cervical cancer deaths (rate, per 100,000)" = cancer_mortality_rate,
        "Cervical cancer diagnosis (rate, per 100,000)" = cancer_diagnosis_rate,
        "Cervical dysplasia diagnosis (rate, per 100,000)" = dysplasia_diagnosis_rate,
        "Any other cause death (rate, per 100,000)" = any_othernoncan_mortality_rate)


grouped_df_month_store %>% print()

write.csv(grouped_df_month_store, paste0(path, "outputs/", 
                                           date_of_run, "_", 
                                           cohort, "/rates_by_month.csv"))


#--------------------------
# RDD 
#--------------------------

# fit any cervical diagnosis rate outcome

diagnoses <- c('cancer_diagnosis_rate', 'dysplasia_diagnosis_rate',
                      'cancer_mortality_rate', 'any_othernoncan_mortality_rate')

labels <- c('Cancer diagnosis', 'Dysplasia disgnosis',
                      'Cancer mortality', 'Any non cancer mortality')

save_names <- c('cancer_diag', 'dysplasia_diag',
                      'cancer_mortality', 'all_non_canc_mortality')


for (i in 1:6){
  
  
  discontinuity_plot_same_grad(outcome_name = diagnoses[i],
                             running_var_name = "dob_month_hpv",
                             cut_off = 36,
                             data = grouped_df_month,
                             xlab = paste0("Birth month"),
                             ylab = paste0(labels[i], 
																					 " rate / 100,000 people"),
                             save_name = paste0(save_names[i], "/", save_names[i], "_discontinuity_plot_month_",  cohort),
                             plot=TRUE)

}


#--------------------------
# Fit full RDD model
#--------------------------

#ADD CATCH TO ONLY DO THIS FOR THE NON/CATCH UP COHORT

#vaccination rate by cohort

lookup_vaccination_rate <- read.csv(paste0(path, "lookups/lookup_cohort_vacc_rate.csv")) %>%
	rename(vaccination_rate_1 = vaccinationrate_1) %>% 
	mutate(vaccination_rate_1 = vaccination_rate_1/100, 
				vaccination_rate_2 = vaccination_rate_2/100,
				vaccination_rate_3 = vaccination_rate_3/100)

output_coef <- data.frame(Value = c("coefficient",
																	 "CI_lower",
																	 "CI_upper",
																	 "p_value", 
																	 "se",
																	 "discontinuty",
                                   "sharp_disc",
																	 "CI_lower_disc",
																	 "CI_upper_disc",
                                   "p_value_disc"))

output_ve <- data.frame(Value = c("ve", "ve_lower", "ve_upper"))


###loop for all outcomes ##

if (grepl("non", name)){

## loop over main RDD for multiple outcomes

output_list <- list()

for (i in 1:length(outcome_list)){
	
output_list[[i]] <- fuzzy_rdd(data = df_analysis, 
					outcome = outcome_list[[i]],
					vaccination_rate = 2,
				 	lookup = lookup_vaccination_rate)

}

outputs <- as.data.frame(do.call(cbind, output_list))

path <- " .../HPV/"

write.csv(outputs, paste0(path, "outputs/", date_of_run, "_", 
                          substr(name, 25, X), 
                          "/",  substr(name, 25, X),
                          "_rdd_store.csv"))
  
  
##run sensitivity tests for dysplasia and cancer diagnosis rates


output_coef <- data.frame(Value = c("coefficient",
																	 "CI_lower",
																	 "CI_upper",
																	 "p_value"))
                          

dysplasia <- list()
cancer <- list()

cutoff <- c(34,35,36,37,38)

for (i in 1:length(cutoff)){
	
dysplasia[[i]] <- fuzzy_cutoffsensitivity(data = df_analysis, 
					outcome = "dysplasia_diagnosis_rate",
					vaccination_rate = 2,
				 	lookup = lookup_vaccination_rate,
          cutoff = cutoff[[i]])
  
cancer[[i]] <- fuzzy_cutoffsensitivity(data = df_analysis, 
					outcome = "cancer_diagnosis_rate",
					vaccination_rate = 2,
				 	lookup = lookup_vaccination_rate,
          cutoff = cutoff[[i]])
}

dysplasia_outputs <- as.data.frame(do.call(cbind, dysplasia))
  
dysplasia_reformat <- dysplasia_outputs %>% 
    select(c(1:2, 4, 6, 8, 10)) %>% 
    rename_with(~paste0(., "_dysplasia"))

cancer_outputs <- as.data.frame(do.call(cbind, cancer))
  
cancer_reformat <- cancer_outputs %>% 
    select(c(1:2, 4, 6, 8, 10)) %>% 
    rename_with(~paste0(., "_cervcancer"))

outputs_cutoff <- cbind(dysplasia_reformat, cancer_reformat)

write.csv(outputs_cutoff, paste0(path, "outputs/", date_of_run, "_", 
                          substr(name, 25, X), 
                          "/",  substr(name, 25, X),
                          "_cuttoff_sensitivity.csv"))

  
}else if (grepl("routine", name)){
  

print("Do not run RDD for routine versus catch-up")
  
  
}
