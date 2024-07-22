library(magrittr)
library(ggplot2)

# fsa_counties <-
#   sf::read_sf(file.path("data-derived", "fsa-counties.fgb"))

usdm_counties <-
  arrow::read_parquet(file.path("data-derived", "usdm-counties.parquet"))

fsa_normal_grazing_periods <-
  arrow::read_parquet(file.path("data-derived", "fsa-normal-grazing-periods.parquet"))

fsa_lfp_eligibility <-
  arrow::read_parquet(file.path("data-derived", "fsa-lfp-eligibility.parquet"))

county_names <- 
  readr::read_csv(file.path("data-derived", "fsa-county-names.csv"),
                  show_col_types=FALSE)

county_names %>%
  dplyr::filter(stringr::str_starts(name, "Missoula")) %>%
  {list(
    dplyr::filter(usdm_counties, FSA_CODE == .$FSA_CODE),
    dplyr::filter(fsa_normal_grazing_periods, FSA_CODE == .$FSA_CODE),
    dplyr::filter(fsa_lfp_eligibility, FSA_CODE == .$FSA_CODE)
  )}

county = "06091"
plot_normal_grazing_w_drought <- function(county, year = 2023) {
  
  title_name <- county_names %>% 
    dplyr::filter(FSA_CODE == county) %$%
    name
  
  if (length(title_name) == 0) {
    return(glue::glue("No Data for Specified County ({county})!"))
  }
  
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

  
  
  grazing %>%
    dplyr::rowwise() %>%
    dplyr::reframe(start = lubridate::as_date(ifelse(lubridate::year(start) == lubridate::year(end), 
                                 lubridate::`year<-`(start, year), 
                                 lubridate::`year<-`(start, year - 1))),
                  end = lubridate::`year<-`(end, year),
                  plot_name)
  
  dat <- fuzzyjoin::fuzzy_left_join(
    drought, grazing,
    by = c(
      "date" = "start",
      "date" = "end"
    ),
    match_fun = list(`>=`, `<=`)
  ) %>% 
    dplyr::filter(
      !is.na(plot_name),
      class >= 2
    ) 
  
  if (nrow(dat) == 0) {
    return(glue::glue("No D2 or Greater Drought in {county}"))
  }
  
  dat <- dat %>% 
    dplyr::mutate(group = with(rle(class), rep(seq_along(values), lengths))) %>%
    dplyr::group_by(group, class, plot_name) %>%
    dplyr::summarize(
      start = min(date), 
      end = max(date),
      .groups = "drop"
    ) %>%
    dplyr::select(class, start, end, plot_name) %>%
    dplyr::mutate(
      # class = paste0("D", class),
      midpoint = start + (end - start) / 2,
      end = end + 1,
      total_days = as.numeric(difftime(end, start, units = "days")),
      weeks = total_days %/% 7,
      days = total_days %% 7,
      n_weeks = ifelse(weeks > 0 & days > 0, paste(weeks, ifelse(weeks == 1, "Week", "Weeks"), "and", days, ifelse(days == 1, "Day", "Days")),
                       ifelse(weeks > 0, paste(weeks, ifelse(weeks == 1, "Week", "Weeks")),
                              paste(days, ifelse(days == 1, "Day", "Days"))))
    ) %>%
    dplyr::filter(!is.na(class))
  
  if (nrow(dat) == 0) {
    return("No drought during this period!")
  }
  
  date_breaks <- dplyr::case_when(
    max(dat$weeks) >= 8 ~ "1 month",
    max(dat$weeks) <= 1 ~ "1 week",
    TRUE ~ "2 weeks"
  )
  
  dat %>% 
    ggplot(aes(y = plot_name, color = class)) +
    geom_segment(aes(x = start, xend = end, 
                     y = plot_name, yend = plot_name), 
                 linewidth = 2,
                 lineend = "butt",
                 linejoin = "mitre") +
    ggrepel::geom_label_repel(
      aes(x = midpoint, y = plot_name, label = n_weeks), 
      min.segment.length = 0,
      color="black",
      size = 3
    ) +
    scale_x_date(date_labels = "%Y-%m-%d", date_breaks = date_breaks) +
    theme_minimal() +
    theme(
      axis.text.x = element_text(angle=45, hjust=1),
      panel.grid.major.x = element_line(color = "gray80", linewidth = 0.5)
    ) +
    scale_color_manual(
      values = list(
        "D0" = "#ffff00",
        "D1" = "#fcd37f",
        "D2" = "#ffaa00",
        "D3" = "#e60000",
        "D4" = "#730000"
      )
    ) + 
    labs(
      color = "USDM Class", 
      y = "Forage Type", 
      x = "", 
      title = glue::glue(
        "Drought During Normal Grazing Period in {title_name} County"
      )
    ) 
}

county_names %>% 
  dplyr::filter(stringr::str_starts(name, "Missoula")) %$%
  plot_normal_grazing_w_drought(FSA_CODE)

plot_normal_grazing_w_drought("01001")
