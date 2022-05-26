#----------------------
# ETL- RWE Data _ API _ RxNorm
# By: Pedram Azimzadeh
# Date: 2022-05-10
#--------------------------------------------------------

# importing required packages
library(readxl)
library(jsonlite)
library(httr)
library(dplyr)
library(sqldf)
#---------------------------------------------------------------------------Source data
# Step-1
# Import excel file in R and create a list of 5 dataframes

ETL_Excel <- function(fname) {

# Worksheets
  sheets <- readxl::excel_sheets(fname)
  tibble <- lapply(sheets, function(x) readxl::read_excel(fname, sheet = x))
  data_frame <- lapply(tibble, as.data.frame)

# Worksheet names
  names(data_frame) <- sheets

# Print data frame
  print(data_frame)
}

# Import the Excel workbook
all_data <- ETL_Excel("RWD_Medication_Diary_Template.xlsx")

# Source sheet
source_sheet <- data.frame(all_data$source)

# Extract a list of all Unique RxCUI values in the source sheet
RxCUI <- unique(all_data$source[4])
RxCUI

# Convert the RxCUI dataframe to a list so we can use it in our API call
RxCUI_list <- as.list(RxCUI)
RxCUI_list

#---------------------------------------------------------------------------API Call

# Define basic variables to use in iterations
# Base URL for using API
base_url <- "https://rxnav.nlm.nih.gov/REST/rxcui/"

# Use API to extract Ingredient names for each RxCUI
RxIngr <- NULL
for (i in 1:nrow(RxCUI)){
  code <- RxCUI$RxCUI[i]
  full_url <- paste0(base_url, code)
  api_call <- httr::GET(full_url)
  api_char <- base::rawToChar(api_call$content)
  api_json <- as.data.frame(jsonlite::fromJSON(api_char, flatten = TRUE))
  RxIngr <- rbind(RxIngr, api_json)
}

# Use API to extract General Cardinality data for each RxCUI
# SINGLE, MULTI or HYBRID, according to the term type and
# the number of ingredients as reflected by the presence of a related TTY=MIN (multiple ingredient) concept

RxCard <- NULL
for (i in 1:nrow(RxCUI)){
  code <- RxCUI$RxCUI[i]
  cardinality_att <- "/property.json?propName=GENERAL_CARDINALITY"
  full_url <- paste0(base_url, code, cardinality_att)
  api_call <- httr::GET(full_url)
  api_char <- base::rawToChar(api_call$content)
  api_json <- as.data.frame(jsonlite::fromJSON(api_char, flatten = TRUE))
  RxCard <- rbind(RxCard, api_json)
}

# Use API to extract unit value for single ingredient products for each RxCUI
# Create a list of URLs for all attributes on RxNorm
for(x in RxCUI_list){Rx_all_att_list <- paste0("https://rxnav.nlm.nih.gov/REST/rxcui/", x, "/allProperties?prop=ATTRIBUTES")}
Rx_all_att_list

# Create a function which requests api for all attributes
get_att_func <- function(x){
    full_url <- x
    api_call <- httr::GET(full_url)
    api_char <- base::rawToChar(api_call$content)
    api_json <- as.data.frame(jsonlite::fromJSON(api_char, flatten = TRUE))
    api_json <- as.data.frame(api_json[-1])
    api_json = setNames(data.frame(t(api_json[,-1])), api_json[,1])
}


# Apply the function to each unique rxcui in our source sheet to get all attributes
df313782 <- get_att_func(Rx_all_att_list[1])
df209459 <- get_att_func(Rx_all_att_list[2])
df1110988 <- get_att_func(Rx_all_att_list[3])
df1049589 <- get_att_func(Rx_all_att_list[4])
df206805 <- get_att_func(Rx_all_att_list[5])
df565143 <- get_att_func(Rx_all_att_list[6])
df669126 <- get_att_func(Rx_all_att_list[7])
df312860 <- get_att_func(Rx_all_att_list[8])
df1049260 <- get_att_func(Rx_all_att_list[9])

# manually add the RxCUI to each dataframe above so we have a unique identifier
# then we will used the unique identifier to join the dataframes

# A technical issue makes us to take additional steps to add the unique identifier column to MULTI ingredient dataframes
# The code below is to resolve that issue
RxCUI1 <- 1110988
df <- data.frame(RxCUI1)
df1110988 <- cbind(df, df1110988)

RxCUI2 <- 1049589
df2 <- data.frame(RxCUI2)
df1049589 <- cbind(df2, df1049589)

RxCUI3 <- 1049260
df3 <- data.frame(RxCUI3)
df1049260 <- cbind(df3, df1049260)

# SINGLE ingredient dataframes are modified (RxCUI column added) in one step only
df313782 <- df313782 %>% mutate(RxCUI = 313782)
df209459 <- df209459 %>% mutate(RxCUI = 209459)
df206805 <- df206805 %>% mutate(RxCUI = 206805)
df565143 <- df565143 %>% mutate(RxCUI = 565143)
df669126 <- df669126 %>% mutate(RxCUI = 669126)


All_signle_dfs <- dplyr::bind_rows(df313782, df209459, df206805, df565143, df669126)
Single_act_ing <- data.frame(cbind(RxCUI = All_signle_dfs$RxCUI, Active_ing_RxCUI = All_signle_dfs$Active_ingredient_RxCUI))

#-------------------------------------------------------------------------Results dataframe

Results_df <- as.data.frame(cbind(CUI = RxCUI$RxCUI, Ingredients = RxIngr$name,
                                  Cardinality = RxCard$propConceptGroup.propConcept.propValue))

str(Results_df)
Results_df$CUI <- as.numeric(unlist(Results_df$CUI))

# Split the results dataframe into single and multi
Split_sing_mul <- split(Results_df, Results_df$Cardinality)

# Convert to dataframe format
Results_df_single <- as.data.frame(Split_sing_mul[2])
# Split Ingredients column to get unit information in separated columns
Results_df_single <- Results_df_single %>% separate(`SINGLE.Ingredients`, c("Ingredient","Numerator","Unit", "Delivery", "Type"), " ")

# Add single ingredient RxCUI to single df
Results_df_single <- sqldf("SELECT * FROM Results_df_single a JOIN Single_act_ing b ON a.`SINGLE.CUI` = b.RxCUI")


# Convert MULTI data to dataframe format for ease of further wrangling
Results_df_multi <- as.data.frame(Split_sing_mul[1])

# Modify the MULTI data to make it fit for unionizing with SINGLE data
Results_df_multi <- setnames(Results_df_multi, old = c("MULTI.CUI", "MULTI.Ingredients", "MULTI.Cardinality"),
                             new = c("SINGLE.CUI", "Ingredient", "SINGLE.Cardinality"))


union1 <- merge(Results_df_single, Results_df_multi, by = "row.names", all = TRUE)
union1

# Merge the columns to tidy-up the dataframe
union2 <- union1 %>% mutate(SINGLE.CUI.x = coalesce(SINGLE.CUI.x,SINGLE.CUI.y))
union3 <- union2 %>% mutate(Ingredient.x = coalesce(Ingredient.x,Ingredient.y))
union4 <- union3 %>% mutate(SINGLE.Cardinality.x = coalesce(SINGLE.Cardinality.x,SINGLE.Cardinality.y))
# Remove duplicate columns
union4 <- union4[, -c(9:11)]
union4 <- union4[-1]

# Join the final API data with Souce data
join_final <- sqldf("SELECT *
      FROM source_sheet a
      LEFT JOIN union4 b
      ON a.RxCUI = b.`SINGLE.CUI.x`")

# Clean up the columns
join_final$Delivery <- as.factor(join_final$Delivery)
join_final$Delivery <- na_if(join_final$Delivery, "[Advil]")
join_final <- join_final[, -c(7:9)]
join_final <- join_final %>% separate(Unit, c("numeratorUnit", "denominatorUnit"), "/")
join_final$denominatorUnit[is.na(join_final$denominatorUnit)] <- 1
colnames(join_final)[7] <- "numeratorValue"
join_final$Medication_Name <- gsub('[0-9.]', '', join_final$Medication_Name)
join_final$Medication_Name<-gsub("MG","",as.character(join_final$Medication_Name))
join_final$Medication_Name<-gsub("Oral Tablet","",as.character(join_final$Medication_Name))
join_final$Medication_Name<-gsub(" ","",as.character(join_final$Medication_Name))
colnames(join_final)[12] <- "Ingredient_Type"
join_final$Ingredient_Type<-gsub("SINGLE","ACTIVE",as.character(join_final$Ingredient_Type))
join_final <- join_final[,-c(13:14)]
join_final$Ingredient_Type[is.na(join_final$Ingredient_Type)] <- "MULTI"

# this command intends to add Active Ingredient RxCUI to the ETL_dataset
# By joining a dataframe containing both RxCUI and Active ingredient RxCUI to the Join_final dataframe
join_final_2 <- sqldf("SELECT * FROM join_final a LEFT JOIN Single_act_ing b ON a.RxCUI = b.RxCUI")
join_final_2 <- join_final_2[,-c(13)]

join_final_2$Medication_Name<-gsub("[Tylenol]","",as.character(join_final_2$Medication_Name), fixed = TRUE)
join_final_2$Medication_Name<-gsub("[Advil]","",as.character(join_final_2$Medication_Name), fixed = TRUE)
join_final_2$Medication_Name<-gsub("[Claritin]","",as.character(join_final_2$Medication_Name), fixed = TRUE)

#----------------------------------------------------------Export ETL Data
ETL_data <- join_final_2
write.csv(ETL_data, "ETL_data_PA.csv", row.names=FALSE)

#---------------------------------------------------------Results Tables


#--------------------------------------------------Table 1.a
Tabl1a <- sqldf("SELECT Subject_ID, RxCUI, Active_ing_RxCUI, Medication_Name FROM ETL_data")

# Export the table into a csv file
write.csv(Tabl1a, "T1.a Ingredient Summary Patient.csv", row.names=FALSE)
#--------------------------------------------------Table 1.b

Tabl1b <- sqldf("SELECT Ingredient_Type, RxCUI, Medication_Name, count(distinct(Subject_ID))
                                         FROM ETL_data
                                         GROUP BY Medication_Name")
# add the percent of total patients column
Tabl1b <- Tabl1b %>% mutate(`%_of_Total_Patients`=((`count(distinct(Subject_ID))`/3)*100))
# limit the number of decimals in the percent of total patients column
Tabl1b <- Tabl1b %>% mutate_if(is.numeric, round, 0)
names(Tabl1b)[names(Tabl1b) == 'count(distinct(Subject_ID))'] <- 'Number_of_unique_Patients_exposed'

# Export the table into a csv file
write.csv(Tabl1b, "T1.b Ingredient Summary Study.csv", row.names=FALSE)


#---------------------------------------------------------------------------Table 2.a

Tabl2a_in_creation <- sqldf("SELECT Subject_ID, Ingredient_Type, RxCUI, Active_ing_RxCUI, Medication_Name, count(Medication_Name), numeratorValue
                FROM ETL_data
                GROUP BY Medication_Name, Subject_ID")
Tabl2a_in_creation$numeratorValue <- as.integer(Tabl2a_in_creation$numeratorValue)

Tabl2a_in_creation <- Tabl2a_in_creation %>% mutate(Avg_Daily_Dose=(`count(Medication_Name)`*`numeratorValue`))

Tabl2a_in_creation <- Tabl2a_in_creation[-7]
Tabl2a_in_creation <- setnames(Tabl2a_in_creation, old = c("Subject_ID", "Ingredient_Type", "RxCUI", "Active_ing_RxCUI", "Medication_Name", "count(Medication_Name)", "Avg_Daily_Dose"),
                               new = c("Subject_ID", "Ingredient_Type", "RxCUI", "Active_ing_RxCUI", "Medication_Name","Number_of_Days_with_Medication_Use", "Average_Daily_Dose_Based _On_Medication_Use"))
Tabl2a <- Tabl2a_in_creation

# Export the table into a csv file
write.csv(Tabl2a, "Tabl2a.csv", row.names=FALSE)


#-----------------------------------------------------------------------------End






