#' Calcualte rates - calculate mortality rates/diagnosis rates for particular time periods from
#' person level sparklyr dataframe
#'
#' @param time_grouping The interval to calcuate the rate over e.g. Month
#'
#' @return grouped_df output with rates per time interval

compute_rates <- function(time_grouping, dataframe){
  time_grouping <- as.symbol(time_grouping)

  # by quarter
  grouped_df <- dataframe %>% 
    group_by(!!time_grouping) %>%
    summarise(total_population = n(),
        total_cerv_can_death = sum(cerv_canc_death),
        total_cerv_can = sum(cerv_can),
        total_cerv_dysp = sum(cerv_dysp),
				total_any_othernoncan_death = sum(any_other_noncanc_death)) %>%
    collect()

  grouped_df <- grouped_df %>%
    mutate(cancer_diagnosis_rate = total_cerv_can/total_population*100000,
        dysplasia_diagnosis_rate = total_cerv_dysp/total_population*100000,
        cancer_mortality_rate = total_cerv_can_death/total_population*100000,
				any_othernoncan_mortality_rate = total_any_othernoncan_death/total_population*100000)
  
  return(grouped_df)
}



#' Discontinuity plot - fit straight lines wiht discontinuity at cut off, with same gradients
#'
#' @param outcome_name Outcome of interest e.g. Cervical cancer
#' @param running_var_name e.g DOB
#' @param cut_off e.g 1-Sept-1990

#'
#' @return saved ggplots


discontinuity_plot_same_grad <- function(outcome_name, running_var_name, cut_off, data, 
                                         xlab = NULL, ylab=NULL, save_name, plot=FALSE){
  
  data <- data %>%
    dplyr::rename(outcome = outcome_name,
        running_var = running_var_name) %>%
    mutate(D = as.factor(ifelse(running_var >= cut_off, 1, 0)))


  # Dyspalsia diagnosis rate at 4.5 year follow-up (non-b, non-a, (discontinuty at 36 months - plot 12-59) at cohort 2, cohort 3)


  # https://bookdown.org/carillitony/bailey/chp11.html
  m <- data %$% 
    #outcome = rate, D = discontinuty, predictor of dob month centred around the discontinuty time-point 
    lm(outcome ~ D + I(running_var - cut_off)) 

  results <- data.frame(tidy(m, conf.int = TRUE, conf.level = 0.95))
  
  write.csv(results, paste0(outputs_folder, "discontinuity_fits_and_plots/", save_name, "_same_grad_results.csv"))
  
  if (plot == TRUE){

    prediction <- predict(m, interval = "confidence")
  
    data_prediction <- cbind(data, prediction)
    
    ggplot(data_prediction, aes(x = running_var, y = outcome , colour = D) ) +
      geom_point() +
      geom_ribbon(aes(ymin = lwr, ymax = upr, x = running_var, fill = D, color=NULL), alpha = 0.3) +
      geom_line(aes(y = fit), size = 1) +
      theme_bw() +
      theme(legend.position = "none",
        text = element_text(size=18)) +
      xlab(xlab) +
      ylab(ylab)
		
      ggsave(paste0(outputs_folder, "discontinuity_fits_and_plots/", save_name, "_same_grad.png"))
    }
  
  return(summary(m))
  
  
}


#' Discontinuity plot - fit straight lines wiht discontinuity at cut off, different gradients
#'
#' @param outcome_name Outcome of interest e.g. Cervical cancer
#' @param running_var_name e.g DOB
#' @param cut_off e.g 1-Sept-1990

#' @return saved ggplots
discontinuity_plot_different_grad <- function(outcome_name, running_var_name, cut_off, data, xlab = NULL, ylab=NULL, 
                                              save_name, plot=FALSE){
  
    data <- data %>%
    rename(outcome = outcome_name,
        running_var = running_var_name) %>%
    mutate(D = as.factor(ifelse(running_var >= cut_off, 1, 0)))
    
  
  # https://bookdown.org/carillitony/bailey/chp11.html
  # model where mutiple linear slopes allowed (based on plot above)
  m <- data %$% 
    #outcome = rate, D = discontinuty, predictor of dob month centred around the discontinuty time-point 
    lm(outcome ~ D * I(running_var - cut_off)) 
  
  results <- data.frame(tidy(m, conf.int = TRUE, conf.level = 0.95))
  
  write.csv(results, paste0(outputs_folder, "discontinuity_fits_and_plots/", save_name, "_different_grad_results.csv"))

  if (plot == TRUE){
    
    prediction <- predict(m, interval = "confidence")
  
    data <- cbind(data, prediction)
    
    ggplot(data, aes(x = running_var, y = outcome , colour = D) ) +
      geom_point() +
      geom_ribbon(aes(ymin = lwr, ymax = upr, x = running_var, fill = D, color=NULL), alpha = 0.3) +
      geom_line(aes(y = fit), size = 1) +
      theme_bw() +
      theme(legend.position = "none",
        text = element_text(size=18)) +
      xlab(xlab) +
      ylab(ylab)
		
      ggsave(paste0(outputs_folder, "discontinuity_fits_and_plots/", save_name, "_different_grad.png"))
  }
  
  return(summary(m))
}


#' Fuzzy RDD 
#'
#' @param outcome Steing specifying outcome e.g. Cervical cancer
#' @param vaccination_rate Numeric
#' @param lookup Lookup with vaccination coverage by cohort

#'
#' @return df with model estimates


fuzzy_rdd <- function(data, outcome, vaccination_rate, lookup){
 
rates_month <- compute_rates("dob_month_hpv", data)
rates_quarter <- compute_rates("dob_quarter_hpv", data)

	
join_link_month <- data %>% select(cohort, dob_month_hpv) %>% distinct()
join_link_quarter <- data %>% select(cohort, dob_quarter_hpv) %>% distinct()

	
#add cohort var to the rates df by timevar
rates_month <- left_join(rates_month, join_link_month, by = paste0("dob_month_hpv"), copy = TRUE)
rates_quarter <- left_join(rates_quarter, join_link_quarter, by = paste0("dob_quarter_hpv"), copy = TRUE)
	
#join lookup to have vaccination rates
rates_month <- left_join(rates_month, lookup, by = "cohort", copy = TRUE)
rates_quarter <- left_join(rates_quarter, lookup, by = "cohort", copy = TRUE)

	
#coverage vax to use
vaccination_var <- paste0("vaccination_rate_", vaccination_rate)
	
rates_month %<>%
			dplyr::rename(outcome_var = all_of(outcome), 
									 vax_var = all_of(vaccination_var)) %>%
		#to calc ve we want log odds of proportion of outcomes, convert back from rate 
			dplyr::mutate(log_odds_outcome = log((outcome_var/100000) / (1 - (outcome_var/100000))))
	
rates_quarter %<>%
			dplyr::rename(outcome_var = all_of(outcome), 
									 vax_var = all_of(vaccination_var)) %>%
		#to calc ve we want log odds of proportion of outcomes, convert back from rate 
			dplyr::mutate(log_odds_outcome = log((outcome_var/100000) / (1 - (outcome_var/100000))))

#https://cran.r-project.org/web/packages/rdrobust/rdrobust.pdf
	
  #rdrobust implements local polynomial Regression Discontinuity (RD) 
	#point estimators with robust bias-corrected confidence intervals
	
#by month
rdd_month <- rdrobust(y=rates_month$outcome_var, 
             							x=rates_month$dob_month_hpv, 
													c=36,
													p=1,
													fuzzy = rates_month$vax_var, 
													kernel="uniform",
													weights = rates_month$total_population)
  
#output_month <- c(Coef = rdd_month$coef[1],
#					  CI_lower = rdd_month$ci[1,1],
#						CI_upper = rdd_month$ci[1,2],
#						p_value = rdd_month$pv[1],
#						se = rdd_month$se[1], 
#					 	discontinuty =  rdd_month$tau_cl[2] - rdd_month$tau_cl[1])
#	
	

#update to include sharp rdd to calc disc w/CI
rdd_month_discont <- rdrobust(y=rates_month$outcome_var, 
             							x=rates_month$dob_month_hpv, 
													c=36,
													p=1,
													kernel="uniform",
													weights = rates_month$total_population)
	
#do we want bias corrected data? e.g for the discontinuity do we want tau_bc?
output_month <- c(Coef = rdd_month$coef[1],
					  CI_lower = rdd_month$ci[1,1],
						CI_upper = rdd_month$ci[1,2],
						p_value = rdd_month$pv[1],
						se = rdd_month$se[1], 
					 	discontinuty =  rdd_month$tau_cl[2] - rdd_month$tau_cl[1],
            sharp_disc = rdd_month_discont$coef[1],
					  CI_lower_disc = rdd_month_discont$ci[1,1],
						CI_upper_disc = rdd_month_discont$ci[1,2],
						p_value_disc = rdd_month_discont$pv[1] )
  

output_month <- cbind(output_coef, output_month)
	
name_table = as.character(paste0(outcome, "_month"))

  output_month <- output_month %>% 
		dplyr::rename(!!name_table := "output_month")
	
rdd_month <- rdrobust(y=rates_month$log_odds_outcome, 
             							x=rates_month$dob_month_hpv, 
													c=36,
													p=1,
													fuzzy = rates_month$vax_var, 
													kernel="uniform",
													weights = rates_month$total_population)
	
output_month_ve <- c(ve = 1 - exp(rdd_month$coef[1]),
					  ve_lower = 1-exp(rdd_month$ci[1,1]),
						ve_upper = 1-exp(rdd_month$ci[1,2]))

output_month_ve <- cbind(output_ve, output_month_ve)
	
	
output_month_ve <- output_month_ve %>% 
			dplyr::rename(!!name_table := "output_month_ve")
	
output_save_month <- rbind(output_month, output_month_ve)


}


#' Sensitivity RDD - change cut-off
#'
#' @param outcome Steing specifying outcome e.g. Cervical cancer
#' @param vaccination_rate Numeric
#' @param lookup Lookup with vaccination coverage by cohort
#' @param cut-off List of cut off values

#'
#' @return df with model estimates

fuzzy_cutoffsensitivity <- function(data, outcome, vaccination_rate, lookup, cutoff){
 
  rates_month <- compute_rates("dob_month_hpv", data)

  join_link_month <- data %>% select(cohort, dob_month_hpv) %>% distinct()

  rates_month <- left_join(rates_month, join_link_month, by = paste0("dob_month_hpv"), copy = TRUE)

  rates_month <- left_join(rates_month, lookup, by = "cohort", copy = TRUE)

  vaccination_var <- paste0("vaccination_rate_", vaccination_rate)

  rates_month %<>%
        dplyr::rename(outcome_var = all_of(outcome), 
                     vax_var = all_of(vaccination_var)) %>%
      #to calc ve we want log odds of proportion of outcomes, convert back from rate 
        dplyr::mutate(log_odds_outcome = log((outcome_var/100000) / (1 - (outcome_var/100000))))

  #by month
  rdd_month <- rdrobust(y=rates_month$outcome_var, 
                            x=rates_month$dob_month_hpv, 
                            c=cutoff,
                            p=1,
                            fuzzy = rates_month$vax_var, 
                            kernel="uniform",
                            weights = rates_month$total_population)

  #do we want bias corrected data? e.g for the discontinuity do we want tau_bc?
  output_month <- c(Coef = rdd_month$coef[1],
              CI_lower = rdd_month$ci[1,1],
              CI_upper = rdd_month$ci[1,2],
              p_value = rdd_month$pv[1])
  

 output_month <- cbind(output_coef, output_month)
	
 name_table = as.character(paste0(cutoff, "_", outcome))

 output_month <- output_month %>% 
		dplyr::rename(!!name_table := "output_month")
	
return(output_month)

}
