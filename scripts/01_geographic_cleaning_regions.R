
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

# Note: region_prev = region name after pre-processing but before fuzzy

# If any region name is named in more than one way consolidate them 
# to one using preprocess
# renaming (see above) - # visual checking (eye-ball after sorted output)
# pre process renaming

## check if there are archived name matches, If there are use them 
fname = paste0("processed_output/region_name_corrected.csv")

# correct names based on archived matches 
if (file.exists(fname)) {
  region_rxv_0<- read_csv(fname, show_col_types = FALSE)
  region_proc<- region_proc |>
    left_join(region_rxv_0, by= c("region"= "region")) |>
    mutate(region_prev = region, 
           region =  correct_name) |>
    distinct(region, region_prev, region_old)
}


## update kebele level data regions based on the archived matches
dat_proc<-
  dat_proc |>
  left_join(region_proc |> rename(region_now = region), c("region"="region_prev", "region_old")) |>
  mutate(region = region_now) |>
  dplyr::select(-region_now)

# capturing zones in new regions
central_eth<- c("Guraghe", "Hadiya", "Halaba", "Kembata Tembaro", "Siltie", "Yem Special")

south_eth<- c("Alle", "Amaro", "Basketo", "Burji", "Derashe", "Gamo", "Gedeo", "Gofa", "Konso", "South Omo", "Wolayita")

# region shift for woredas and zones
region_shift <- dat_proc |>
  mutate(region_shift = case_when (zone %in% central_eth ~ "Central",
                                   zone %in% south_eth ~ "South",
                                   #correct region names based on reference  shapefile
                                   woreda %in% c("East Telemt", "West Telemt", "Maytsebry Town") ~ "Tigray",
                                   woreda %in% c("Berehet") & zone =="North Shewa" ~ "Oromia",
                                   TRUE ~ "No shift"))   |>
  filter(region_shift != "No shift") |>
  dplyr::select(keb_id, region_old, region_shift) 

# implement fuzzy join based on region names in the two datasets (shapfile and api data)
## all thse fuzzy matches are candidate matches. They are subject to finetuned matches based on minimum distance 

region_fuzzy<- region_proc |> 
  stringdist_join(ref_regions,# fuzzy join
                  by = c("region"="correct_name"),  
                  distance_col = "dist",
                  max_dist = 10,
                  ignore_case = TRUE)   

# the following script picks the match that has the smallest character distance both 
# for the candidate name (api region name) and the target name (reference names)
region_fuzzy_match<- region_fuzzy |> 
  group_by (region) |>
  # select the closest for each source region
  slice_min(dist, n=1, with_ties = TRUE)  |>
  group_by (correct_name) |>
  # select the closest for each target region
  slice_min(dist, n=1, with_ties = TRUE)  |>
  arrange(id)

## isolating region names that did not match either due to ending up 
## with duplicate matches or not matching at all 
# When non-empty outputs are observed, they will be subject to pre-procesing corrections 
duplicate_match <- data.frame(table(region_fuzzy_match$region, region_fuzzy_match$correct_name))|>
  filter(Freq>0) |>
  group_by(Var1) |>
  summarize(count = n()) |>
  filter(count>1)

# isolate mismatches
mis_match <- region_proc |>
  left_join (region_fuzzy_match, by= c("region", "region_old")) |>
  filter(is.na (correct_name))

if (nrow(mis_match)== 0 & nrow(duplicate_match)==0) message("All region names in API succesfully matched to unique region names in the shapefile." )  

if(nrow(mis_match)>0)  message(
  paste0(
    nrow(mis_match|> distinct(region))," region names in API have no matches.  Make sure these regions have consistent names in preprocess. \n",
    paste( as.vector(t(data.frame(mis_match |> distinct(region) |> mutate(sep="\n ")))), collapse = "-") ))

if(nrow(duplicate_match)>0)  message(paste0(
  length(unique(duplicate_match$Var1))," region names in API have multiple matches. Check these names. \n",
  paste(unique(duplicate_match$Var1), collapse = "\n") )) 

if ( (nrow(region_fuzzy_match |> filter(dist>0)))==0) message("All region name matching handled during pre-processing (pre-fuzzy).")

#- archive fuzzy correction geographies ------------------------------------------------
region_corrected<- data.frame(region = region_fuzzy_match$region,
                              correct_name = region_fuzzy_match$correct_name) |>
  distinct(region, correct_name)

# save the corrections. If file exists open if not create
if (file.exists(fname)) {
  region_rxv_0 <- read_csv(fname, show_col_types = FALSE)
  region_rxv<-  bind_rows(region_rxv_0, region_corrected) |>
    distinct(region, correct_name)
  message( paste0(length(unique(region_proc$region)), " regions matched succesfully. ",
                  nrow(region_rxv) - nrow(region_rxv_0)," new names archived."))  
} else {
  write_csv(region_corrected , fname)
  message( paste0(nrow(region_fuzzy_match |> distinct(region)), " regions matched succesfully and archived."))
}


dat_region_out<- list(
  clean_df = dat_proc |>
    left_join (region_fuzzy_match, by= c("region", "region_old")) |>
    ## add regions for woredas that had region shift
    left_join (region_shift, by = c("region_old", "keb_id")) |> 
    mutate(correct_name = case_when(is.na(region_shift) ~ correct_name,
                                    TRUE ~ region_shift)) |>
    dplyr::select(-c(region, region_shift)) |>
    rename(region_new = correct_name),
  mis_macth = mis_match,
  duplicate_match = duplicate_match)


# visualize name matching perfect matching between pro-proc and target
dat_region_out$clean_df |>
  ggplot(aes(x= region_new, y = dist)) +
  geom_boxplot() +
  coord_flip()  

ggsave(filename = "plots/eth-api_region_names_matched.tiff",
       width = 8, height = 6, compression = "lzw", bg="white")

## cross-tabulate pre-processed and fuzzy corrected regions

# rows are correct names and column names are old names
table(dat_region_out$clean_df$region_new, dat_region_out$clean_df$region_old) # row 

## names are correct names, column are the names in the API
## capture clean regions for integrating with output   

region_out<- dat_region_out$clean_df |>
  dplyr::select(region_new, zone, woreda, region_old, kebele, keb_id)

# save the clean output
write_csv(region_out, paste0("processed_output/eth_api_clean_region_names.csv"))
