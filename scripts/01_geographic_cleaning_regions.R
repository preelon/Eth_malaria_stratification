
# this first script will be for cleaning region names in the kebele level API dataset. 

rm(list = ls())

# step1- load libraries
library(tidyverse)
library(lubridate)
library(fuzzyjoin)

#- reference geog names --------------------------------------------------------

# step2- load reference names 

ref_names<- readRDS("data/eth-kebele-names-shapefile-clean.RDS") |>
  distinct(region, zone, woreda, kebele) |> as_tibble() 

# generate the reference region names 
ref_regions <- ref_names |> 
  distinct(region) |>
  rename(correct_name= region) |>
  mutate(id =row_number())

#step3 - load kebele level data names---------------------------------------------
# pre-fuzzy = process corrections identified either by loading 
# previously corrected names or new identified 
# load existing datasets, either upstream clean data or  initial raw data and implement  known changes to the variable to be cleaned 

dat = read_csv("data/eth_kebele_2015_cases_sw_som_added_tig_adj.csv") |>
  mutate(region = str_to_title(region),
         zone =  str_to_title(zone),
         woreda= str_to_title(woreda)) |>
  filter(!is.na(region) & !is.na(zone) &  !is.na(woreda) & !is.na(kebele)) |>
  distinct(region, zone, woreda, kebele) |>
  mutate(keb_id = row_number())

# lets see the unique regional names so that we can decide the preprocess
# steps we should take next
unique(dat$region)

# 4. Pre-processing
# apply known name changes and generate (aggregate) unique names to be corrected
# this part mainly focuses on having consistent region names
dat_proc <-
  dat |>
  mutate(region_old = region) |>
  mutate(region = gsub (" Ethiopia",  "", region))|>
  mutate(region  = case_when(grepl("Swepr", region) | grepl("Swer", region) |  grepl("Swe", region) ~ "South West",
                             region %in% c("B.G", "B.G.", "BG", "B. Gumuz", "Benishangul Gumz", "Benshangul Gumz") ~ "Benishangul Gumuz" ,
                             region %in% c("CE", "C.E" , "C.E.") ~ "Central",
                             TRUE ~ region))

## archive pre-fuzzy geography names
region_proc<-
  dat_proc |>
  distinct(region, region_old)  |>
  filter(!is.na(region)) |>
  mutate(region_prev = region) |> #what is the purpose of this mutation?
  mutate(rid = row_number())


# If any region name is named in more than one way consolidate them 
#to one using preprocess
# renaming (see above) - # visual checking (eye-ball after sorted output)
# pre process renaming

## check if there are archived name matches, If there are use them 
fname = paste0("processed-output/region_name_corrected.csv")

# correct names based on archived matches 
if (file.exists(fname)) {
  region_rxv_0<- read_csv(fname, show_col_types = FALSE)
  region_proc<- region_proc |>
    left_join(region_rxv_0, by= c("region"= "region")) |>
    mutate(region_prev = region, 
           region =  correct_name) |>
    distinct(region, region_prev, region_old)
}