library(magrittr)
library(ggplot2)

fsa_counties <-
  sf::read_sf(file.path("data-derived", "fsa-counties.fgb"))

usdm_counties <-
  arrow::read_parquet(file.path("data-derived", "usdm-counties.parquet"))

fsa_normal_grazing_periods <-
  arrow::read_parquet(file.path("data-derived", "fsa-normal-grazing-periods.parquet"))

fsa_lfp_eligibility <-
  arrow::read_parquet(file.path("data-derived", "fsa-lfp-eligibility.parquet"))

county = "30063"
plot_normal_grazing_w_drought <- function(county) {
  
  drought <- usdm_counties %>% 
    dplyr::filter(FSA_CODE == county) %>%
    dplyr::select(date=Date, class=`USDM Class`)

  
   grazing <- fsa_normal_grazing_periods %>% 
    dplyr::filter(FSA_CODE == county) %>% 
    dplyr::rowwise() %>% 
    dplyr::mutate(
      names = stringr::str_split_1(`Type Name`, pattern = "; ") %>% 
      unique() %>% 
      {
        ifelse(
          length(.) > 2,
          glue::glue('{paste(c(.[[1]], .[[2]]), collapse = "; ")}; {length(.) - 2} others...'), 
          paste(., collapse = ";")
        )
      },
      l = stringr::str_split_1(`Type Name`, pattern = "; ") %>% 
        unique() %>% 
        length()
    ) %>% 
    dplyr::mutate(
      plot_name = glue::glue(
        "{`Crop Name`} [{names}]"
      )
    ) %>% 
    dplyr::group_by(
      start = `Grazing Period Start Date`,
      end = `Grazing Period End Date`
    ) %>% 
    dplyr::summarise(
      plot_name = paste(plot_name, collapse = "\n")
    ) %>% 
    dplyr::arrange(plot_name) %>% 
    dplyr::mutate(plot_name = factor(plot_name))
  
  fuzzyjoin::fuzzy_left_join(
    drought, grazing,
    by = c(
      "date" = "start",
      "date" = "end"
    ),
    match_fun = list(`>=`, `<=`)
  ) %>% 
    dplyr::filter(!is.na(plot_name)) %>% 
    dplyr::group_by(start, end, plot_name) %>% 
    dplyr::summarise(class = max(class), .groups="drop") %>%
    dplyr::mutate(class = factor(
      class, 
      levels = c(0, 1, 2, 3, 4)
    )) %>%
    ggplot(aes(y = plot_name, color = class)) +
    geom_segment(aes(x = start, xend = end, 
                     y = plot_name, yend = plot_name), size = 2) +
    scale_x_date(date_labels = "%Y-%m-%d", date_breaks = "1 month") +
    theme_minimal() +
    theme(
      axis.text.x = element_text(angle=45, hjust=1),
      panel.grid.major.x = element_line(color = "gray80", size = 0.5)
    ) +
    scale_color_manual(
      values = list(
        "0" = "#ffff00",
        "1" = "#fcd37f",
        "2" = "#ffaa00",
        "3" = "#e60000",
        "4" = "#730000"
        )
    ) + 
    labs(
      color = "USDM Class", 
      y = "Forage Type", 
      x = "", 
      title = glue::glue(
        "Max Drought Class During Normal Grazing Period"
      )
    ) 
}

