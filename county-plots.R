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
    dplyr::select(date=Date, class=`USDM Class`) %>% 
    tidyr::complete(date = seq(min(date), max(date), by = "1 day")) %>%
    tidyr::fill(class, .direction = "down")
  
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
      plot_name = paste(plot_name, collapse = "\n"),
      .groups = "drop"
    ) %>% 
    dplyr::arrange(plot_name) %>% 
    dplyr::mutate(plot_name = factor(plot_name)) 
    # tidyr::pivot_longer(-plot_name, values_to = "date") %>% 
    # dplyr::select(-name) %>%
    # dplyr::group_by(plot_name) %>% 
    # tidyr::complete(
    #   date = seq(min(date), max(date), by = "1 day")
    # )
  
  dat <- fuzzyjoin::fuzzy_left_join(
    drought, grazing,
    by = c(
      "date" = "start",
      "date" = "end"
    ),
    match_fun = list(`>=`, `<=`)
  ) %>% 
    dplyr::filter(!is.na(plot_name)) %>% 
    dplyr::mutate(group = with(rle(class), rep(seq_along(values), lengths))) %>%
    dplyr::group_by(group, class, plot_name) %>%
    dplyr::summarize(start = min(date), end = max(date), .groups = "drop") %>%
    dplyr::select(class, start, end, plot_name) %>%
      dplyr::mutate(
        class = factor(
          class, 
          levels = c(0, 1, 2, 3, 4)
        ),
        end = end + 1
    ) %>%
    dplyr::filter(!is.na(class))
  
  if (nrow(dat) == 0) {
    return("No drought during this period!")
  }
  
  dat %>% 
    ggplot(aes(y = plot_name, color = class)) +
    geom_segment(aes(x = start, xend = end, 
                     y = plot_name, yend = plot_name), 
                 linewidth = 2,
                 lineend = "butt",
                 linejoin = "mitre") +
    scale_x_date(date_labels = "%Y-%m-%d", date_breaks = "1 month") +
    theme_minimal() +
    theme(
      axis.text.x = element_text(angle=45, hjust=1),
      panel.grid.major.x = element_line(color = "gray80", linewidth = 0.5)
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
        "Drought During Normal Grazing Period"
      )
    ) 
}

