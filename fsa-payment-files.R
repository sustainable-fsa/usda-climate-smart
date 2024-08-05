# https://www.fsa.usda.gov/news-room/efoia/electronic-reading-room/frequently-requested-information/payment-files-information/index

library(tidyverse)
library(openxlsx2)
options("openxlsx2.maxWidth" = 64)

fsa_payments <-
  list.files("data-raw/fsa-payment-files",
           full.names = TRUE) %>%
  purrr::map_dfr(readxl::read_excel) %>%
  dplyr::mutate(`State FSA Code` = stringr::str_pad(`State FSA Code`, width = 2, side = "left", pad = "0"),
                `County FSA Code` = stringr::str_pad(`State FSA Code`, width = 3, side = "left", pad = "0"),
                FSA_CODE = paste0(`State FSA Code`, `County FSA Code`)) %>%
  dplyr::select(!c(`Delivery Point Bar Code`, `Payment Date`, `State FSA Code`, `County FSA Code`)) %>%
  dplyr::select(FSA_CODE,
                County = `County FSA Name`,
                State = `State FSA Name`,
                Year = `Accounting Program Year`,
                Name = `Formatted Payee Name`,
                `Mail To` = `Address Information Line`,
                Address = `Delivery Address Line`,
                City = `City Name`,
                ST = `State Abbreviation`,
                Zip = `Zip Code`,
                Disbursement = `Disbursement Amount`,
                `Program Code` = `Accounting Program Code`,
                Program = `Accounting Program Description`) %>%
  dplyr::filter(`Program Code` %in% c(
    2435:2439,2444, 2446:2448, 2571, 2775, 2833, 2835, 2911, 2933
  )) %>%
  dplyr::group_by(across(c(-Disbursement))) %>%
  dplyr::summarise(Disbursement = sum(Disbursement, na.rm = TRUE), .groups = "drop")

# fsa_payments %>%
#   dplyr::group_by(Year, State) %>%
#   dplyr::summarise(Disbursement = sum(Disbursement, na.rm = TRUE), .groups = "drop") %>%
#   # dplyr::filter(State %in% c("Washinton", 
#   #                            "Montana", 
#   #                            "Idaho", 
#   #                            "Oregon",
#   #                            "Wyoming",
#   #                            "Alaska"),
#   #               Year %in% 2020:2023
#   # ) %>%
#   dplyr::arrange(State, Year) %>%
#   tidyr::pivot_wider(names_from = Year,
#                      values_from = Disbursement) %>%
#   kableExtra::kable(format = "simple")


class(fsa_payments$Disbursement) <- "currency"

wb_workbook(creator = "Kyle Bocinsky",
            title = "UMRB FSA Payments") %>%
  wb_add_worksheet("Payments") %>%
  wb_add_data_table("Payments", 
                    x = fsa_payments %>%
                      dplyr::arrange(FSA_CODE, City, ST, Zip, Address, Year) %>%
                      dplyr::filter(State %in% c("Montana",
                                                 "Idaho",
                                                 "North Dakota",
                                                 "Wyoming",
                                                 "South Dakota",
                                                 "Nebraska",
                                                 "Iowa")),
                    table_style = "TableStyleLight1",
                    na.strings = ""
  ) %>%
  wb_freeze_pane(first_row = TRUE) %>%
  wb_add_cell_style(
    dims = "A1:M7000",
    wrap_text = "1"
  ) %>%
  wb_set_col_widths(cols = 1:13,
                    widths = "auto") %>%
  wb_save(file = "data-derived/fsa-payment-files-umrb-2017.xlsx")
