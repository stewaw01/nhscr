# Load  packages ----
library(dplyr)
library(tidyr)
library(readr)
library(odbc)
library(lubridate)
library(openxlsx)
library(janitor)
library(keyring)
library(gdata)
library(stringr)
library(here)

# Function for removing NA and collapsing the columns for SQL query
remove_na_and_collapse <- function(df, col){
  filtered <- df[col] |> filter(!(is.na(df[[col]])))
  paste(as.list(filtered[[col]]), collapse = ",")
}



# Set file input path ----
in_path   <- "/conf/bss/nhscr/2025/cn1jun25/data/"

## Create output path
temp_path <- str_split_1(in_path, pattern = "\\/+")
out_path <- paste(here(), temp_path[5], temp_path[6], sep = "/")

## Create directory for output path if it doesn't already exist
if (!dir.exists(out_path)) {
  dir.create(out_path, recursive = TRUE)
}

# Read data file ----
input_data_file <- read_fwf(paste0(in_path, temp_path[6], ".dat"),
                            fwf_widths(c(17, 15, 15, 15, 15, 15, 15, 1, 8, 6, 6, 5, 20, 8, 6, 25, 4, 6, 25, 6, 9),
                                       col_names = c("nhs_no",
                                                     "surname",
                                                     "maiden_name",
                                                     "second_forname",
                                                     "forename",
                                                     "middle",
                                                     "middle2",
                                                     "sex",
                                                     "date_of_birth",
                                                     "date_of_death",
                                                     "unknown",
                                                     "unknown2",
                                                     "chi_no",
                                                     "postcode",
                                                     "unknown3",
                                                     "patient_id",
                                                     "no_of_studies",
                                                     "study_no",
                                                     "survey_ref",
                                                     "second_study_no",
                                                     "second_survey_ref"
                                                     )), show_col_types = FALSE)

# Print initial rows in input
print(paste0("Number of rows in input file: ", nrow(input_data_file)))

second_entries <- input_data_file |> filter(!is.na(second_study_no)) |> 
  select(-c(study_no, survey_ref)) |> 
  rename(study_no = second_study_no, survey_ref = second_survey_ref)

second_entries["second_study_no"] <- NA
second_entries["second_survey_ref"] <- NA

input_data_file <- input_data_file |> rbind(second_entries)

input_data_file <- transform(input_data_file, chi_no = substr(chi_no, 1, 10), nhs_no_alt = substr(chi_no, 11, 20)) |> 
  mutate(across(where(is.character), ~ na_if(.,"")))

# Print rows after appending secondary surveys
print(paste0("Number of rows in input file (after adding second surveys): ", nrow(input_data_file)))

# SMR Data ----
keyring::keyring_unlock(keyring = "DATABASE",
                        password = source("~/database_keyring.R")[["value"]])

SMRAConnection <- dbConnect(odbc(),
                            dsn = "SMRA",
                            uid = Sys.info()[["user"]], # Assumes the user's SMR01 username is the same as their R server username
                            pwd = keyring::key_get("SMRA", Sys.info()[["user"]], keyring = "DATABASE"))


# Returns all fields for the patients with CHI OR UPI OR PATIENT ID
smr06_output <- as_tibble(
  dbGetQuery(
    SMRAConnection, paste0(
      "
    SELECT *
    FROM ANALYSIS.SMR06_PI
    WHERE UPI_NUMBER IN (", remove_na_and_collapse(input_data_file, "chi_no") ,")
    OR CHI_NO IN (", remove_na_and_collapse(input_data_file, "chi_no") ,")
    OR PE_PATIENT_ID IN (", remove_na_and_collapse(input_data_file, "patient_id") ,")
    "
    )
  )
) |> 
  clean_names()


# Format input file data to specified columns ----
input_data_file_formatted <- input_data_file |> 
  select(study_no,
         surname,
         forename,
         date_of_birth,
         nhs_no,
         sex,
         survey_ref,
         date_of_death,
         chi_no) |> 
  rename_with(~paste0(., "_input"))

# Format SMR06 data to specified columns ----
smr06_output_formatted <- smr06_output |> 
  select(link_no,
         pe_patient_id,
         surname,
         first_forename,
         previous_surname,
         date_of_birth,
         nhs_no,
         sex,
         hosp_gp_diag,
         incidence_date,
         site_icd9,
         icd10s_cancer_site,
         hist_ver,
         marital_status,
         postcode,
         hb_residence_number,
         old_registry,
         occupation,
         type_icdo,
         morph_morphology,
         type_icdo3,
         #indep_primary_tumour,
         tumour_no,
         therapy_objectives,
         upi_number) |> 
  rename_with(~paste0(., "_smr06"))

# Join on chi/upi -> convert dob, dod, and incidence_date to date, and split into separate columns with '/' dividers
output_data <- left_join(input_data_file_formatted, smr06_output_formatted, join_by("chi_no_input" == "upi_number_smr06")) |> 
  mutate(day_of_birth_input = format(dmy(date_of_birth_input), "%d"),
         birth_divide_1 = "/",
         month_of_birth_input = format(dmy(date_of_birth_input), "%m"),
         birth_divide_2 = "/",
         year_of_birth_input = format(dmy(date_of_birth_input), "%y"),
         
         day_of_death_input = format(dmy(date_of_death_input), "%d"),
         death_divide_1 = "/",
         month_of_death_input = format(dmy(date_of_death_input), "%m"),
         death_divide_2 = "/",
         year_of_death_input = format(dmy(date_of_death_input), "%y"),
         
         day_of_birth_smr06 = format(ymd(date_of_birth_smr06), "%d"),
         birth_divide_3 = "/",
         month_of_birth_smr06 = format(ymd(date_of_birth_smr06), "%m"),
         birth_divide_4 = "/",
         year_of_birth_smr06 = format(ymd(date_of_birth_smr06), "%y"),
         
         incidence_day_smr06 = format(ymd(incidence_date_smr06), "%d"),
         incidence_divide_1 = "/",
         incidence_month_smr06 = format(ymd(incidence_date_smr06), "%m"),
         incidence_divide_2 = "/",
         incidence_year_smr06 = format(ymd(incidence_date_smr06), "%Y"),
         
         pe_patient_id_smr06 = as.character(sprintf("%08d", pe_patient_id_smr06)), # Pad patient_id with 0's up to 8 characters
         
         hist_ver_smr06 = case_when(hist_ver_smr06 == 1 ~ "YES", hist_ver_smr06 == 2 ~ "NO"),
         
         site_code_combined = if_else(is.na(site_icd9_smr06), icd10s_cancer_site_smr06, site_icd9_smr06), # Output prioritises icd9 over icd10
         type_code_combined = if_else(is.na(type_icdo_smr06), if_else(is.na(type_icdo3_smr06), morph_morphology_smr06, type_icdo3_smr06), type_icdo_smr06),
         
         therapy_objectives_smr06 = as.character(therapy_objectives_smr06),
         
         sex_smr06 = case_when(sex_smr06 == 1 ~ "M", sex_smr06 == 2 ~ "F")) |>
  
  select(-c(date_of_birth_input, date_of_death_input, date_of_birth_smr06, incidence_date_smr06)) |> 
  distinct()

# TODO - make sure DOB matches since it's in twice from in/smr06

# Create output in the correct format ----
output_data <- output_data |> 
  select(study_no_input,
         sex_input,
         survey_ref_input,
         day_of_death_input,
         death_divide_1,
         month_of_death_input,
         death_divide_2,
         year_of_death_input,
         pe_patient_id_smr06,
         sex_smr06,
         hosp_gp_diag_smr06,
         incidence_day_smr06,
         incidence_divide_1,
         incidence_month_smr06,
         incidence_divide_2,
         incidence_year_smr06,
         site_code_combined,
         hist_ver_smr06,
         marital_status_smr06,
         therapy_objectives_smr06,
         type_code_combined,
         old_registry_smr06
         )


# Filter and save a file for each study number ----
for (c in unique(output_data$study_no_input)) {
  filtered_data <- output_data |>
    filter(study_no_input == c)
  
  file_out_path <- paste0(
    out_path,
    "/nhscr_anon_extract_",
    c,
    ".txt"
  )
  
  write.fwf(filtered_data, file = file_out_path, rownames = FALSE, colnames = FALSE, width = c(7, 2, 26, 2, 1, 2, 1, 5, 9, 2, 6, 2, 1, 2, 1, 5, 7, 11, 3, 3, 7, 5),
            na = " ", sep = "", eol = "\n", justify = "left")
}

# End SMR06 connection ----
dbDisconnect(SMRAConnection)


# Create summary file ----
summary_file <- paste0(out_path, "/nhscrwrite_sum.txt")


date <- paste0("DATE - ", Sys.Date())

time <- paste0("TIME - ", str_extract(Sys.time(), "([0-9\\:]+?)(\\.[0-9]+?$)", group = 1)) # I'm not sure how to account for BST, but does it matter?

divider <- "--------------------------------------------------\n"

header1 <- "SUMMARY REPORT FOR NHSCR-CANCER REGISTRATIONS:-\n"
header2 <- "SEPARATE FILES HAVE BEEN GENERATED FOR:-\n"
table_headers <- "No.     STUDY#     #INPUT     #OUTPUT"


input_numbers <- input_data_file_formatted |>
  select(study_no_input) |>
  group_by(study_no_input) |>
  mutate("#INPUT" = n()) |>
  slice(1) |> 
  rename("STUDY#" = study_no_input)

output_numbers <- output_data |>
  select(study_no_input) |>
  group_by(study_no_input) |>
  mutate("#OUTPUT" = n()) |>
  slice(1) |> 
  rename("STUDY#" = study_no_input)

combined_numbers <- input_numbers |> 
  left_join(output_numbers) |> 
  tibble::rowid_to_column("No.") |> 
  mutate(No. = str_pad(string = No., width = 3, pad = " ")) |> 
  mutate("STUDY#" = str_pad(string = `STUDY#`, width = 10, pad = " ")) |>
  mutate("#INPUT" = str_pad(string = `#INPUT`, width = 12, pad = " ")) |>
  mutate("#OUTPUT" = str_pad(string = `#OUTPUT`, width = 12, pad = " "))


write_lines(c(date, time, divider, header1, header2, table_headers), summary_file)


for (c in unique(combined_numbers$`STUDY#`)) {
  filtered_data <- combined_numbers |>
    filter(`STUDY#` == c)
  
  write_lines(paste0(filtered_data$No., filtered_data$`STUDY#`, filtered_data$`#INPUT`, filtered_data$`#OUTPUT`), summary_file, append = TRUE)
}

