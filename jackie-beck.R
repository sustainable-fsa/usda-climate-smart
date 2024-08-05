library(tidyverse)
library(openxlsx2)
options("openxlsx2.maxWidth" = 64)

fsa_county_names <-
  sf::read_sf(file.path("data-derived", "fsa-county-names.parquet")) %>%
  dplyr::filter(State %in% c("Montana", 
                             "Wyoming",
                             "North Dakota",
                             "South Dakota",
                             "Nebraska",
                             "Iowa"))

fsa_lfp_eligibility <-
  arrow::read_parquet(file.path("data-derived", "fsa-lfp-eligibility.parquet")) %>%
  dplyr::inner_join(fsa_county_names, .) %>%
  dplyr::filter(Year == 2017)
  

fsa_lfp_payments <-
  arrow::read_parquet(file.path("data-derived", "fsa-farm-payments.parquet")) %>%
  dplyr::filter(`Accounting Program Year` == 2017,
                `Accounting Program Code` == 2835) %>%
  dplyr::inner_join(fsa_county_names, .)


class(fsa_payments$Disbursement) <- "currency"

wb_workbook(creator = "Kyle Bocinsky",
            title = "UMRB LFP Data") %>%
  wb_add_worksheet("UMRB Counties") %>%
  wb_add_data_table("UMRB Counties", 
                    x = fsa_county_names,
                    table_style = "TableStyleLight1",
                    na.strings = ""
  ) %>%
  wb_freeze_pane(sheet = "UMRB Counties",
                 first_row = TRUE) %>%
  wb_set_col_widths(sheet = "UMRB Counties",
                    cols = 1:3,
                    widths = "auto") %>%
  wb_add_worksheet("LFP Eligibility") %>%
  wb_add_data_table("LFP Eligibility", 
                    x = fsa_lfp_eligibility,
                    table_style = "TableStyleLight1",
                    na.strings = ""
  ) %>%
  wb_freeze_pane(sheet = "LFP Eligibility",
                 first_row = TRUE) %>%
  wb_set_col_widths(sheet = "LFP Eligibility",
                    cols = 1:8,
                    widths = "auto") %>%
  wb_add_worksheet("LFP Payments") %>%
  wb_add_data_table("LFP Payments", 
                    x = fsa_lfp_payments,
                    table_style = "TableStyleLight1",
                    na.strings = ""
  ) %>%
  wb_freeze_pane(sheet = "LFP Payments",
                 first_row = TRUE) %>%
  wb_add_cell_style(
    sheet = "LFP Payments",
    dims = "A1:N7000",
    wrap_text = "1"
  ) %>%
  wb_set_col_widths(sheet = "LFP Payments",
                    cols = 1:14,
                    widths = "auto") %>%
  wb_save(file = "data-derived/fsa-lfp-data-umrb-2017.xlsx")
