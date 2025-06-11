library(tidyverse)
library(magrittr)

# source("county-data.R")

# Sys.setenv("AWS_ACCESS_KEY_ID"     = keyring::key_get("aws_access_key_id"),
#            "AWS_SECRET_ACCESS_KEY" = keyring::key_get("aws_secret_access_key"),
#            "AWS_DEFAULT_REGION"    = "us-west-2")
# 
# arrow::s3_bucket("sustainable-fsa/usdm-archive/county/parquet/") |>
#   arrow::open_dataset() |>
#   dplyr::filter(date == "2024-12-31") |>
#   dplyr::collect()
#   
# 
# sf::st_read(dsn = "/vsis3/sustainable-fsa/usdm-archive/usdm/parquet/")

usdm_counties <-
  list.files("~/Desktop/usdm-archive/county",
             full.names = TRUE) %>%
  magrittr::set_names(., tools::file_path_sans_ext(basename(.))) %>%
  purrr::map_dfr(arrow::read_parquet,
                 .id = "Date") %>%
  dplyr::mutate(Date = lubridate::as_date(Date))

county_names <- 
  arrow::read_parquet(file.path("data-derived", "fsa-county-names.parquet"))

fsa_normal_grazing_periods <-
  arrow::read_parquet(file.path("data-derived", "fsa-normal-grazing-periods.parquet")) %>%
  dplyr::group_by(FSA_CODE) %>%
  dplyr::summarise(
    `Grazing Period Start Date` = min(`Grazing Period Start Date`),
    `Grazing Period End Date` = max(`Grazing Period End Date`)
  )

county_drought_calendar <-
  function(county = "30063", 
           years = 2000:lubridate::year(today()), 
           width = 8){
    
    # years <- 2000:2024
    # county <- "30063"
    
    calendar_data <-
      usdm_counties %>%
      dplyr::filter(FSA_CODE == county) %>%
      dplyr::ungroup() %>%
      {
        dplyr::left_join(
          tibble::tibble(
            Date = 
              seq(from = min(lubridate::floor_date(.$Date, unit = "year")),
                  to = max(lubridate::ceiling_date(.$Date, unit = "year")),
                  by = "1 day")
          ),
          .
        )
      } %>%
      tidyr::fill(`USDM Class`) %>%
      dplyr::mutate(Year = lubridate::year(Date) %>%
                      as.integer(),
                    yday = lubridate::yday(Date),
                    Month = lubridate::month(Date, label = TRUE),
                    Week = lubridate::epiweek(Date),
                    Week = ifelse(yday <= 7 & Week >= 52, 0, Week),
                    Week = ifelse(yday >= 359 & Week == 1,53, Week),
                    Day = lubridate::wday(Date, label = TRUE),
                    wday = lubridate::wday(Date)) %>%
      dplyr::group_by(Year) %>%
      dplyr::mutate(base = Week[1] > 0,
                    Week = ifelse(base, Week - 1, Week),
                    Week = Week %>%
                      as.character() %>%
                      factor(., levels = 0:53, labels = 1:54, ordered = TRUE)
      ) %>%
      dplyr::ungroup() %>%
      dplyr::filter(Date <= today())
    
    years %>%
      {
        split(.,       
              cut(., 
                  breaks = seq(min(.), max(.) + width, by = width), 
                  include.lowest = TRUE,
                  right = FALSE)
        )
      } %>%
      purrr::map(
        function(years){
          calendar_data %<>%
            dplyr::filter(Year %in% years)
          
          month_breaks <-
            calendar_data %>%
            dplyr::filter(lubridate::mday(Date) == 1,
                          Month != "Jan")
          
          # lab_breaks <-
          #   calendar_data %>%
          #   dplyr::filter(lubridate::mday(Date) == 1,
          #                 Year == min(Year)) %>%
          #   dplyr::mutate(Week = ifelse(wday != 1, as.integer(Week) + 1, Week),
          #                 Week = ifelse(Month == "Jan", 1, Week)) %$%
          #   Week
          
          lab_breaks <-
            c(Jan = 3,
              Feb = 7,
              Mar = 11,
              Apr = 15,
              May = 20,
              Jun = 24,
              Jul = 29,
              Aug = 33,
              Sep = 38,
              Oct = 42,
              Nov = 46,
              Dec = 51
            )
          
          lw = 0.8
          
          grid::grid.newpage()
          
          p <- 
            calendar_data %>%
            # tail(20)
            ggplot() +
            geom_tile(aes(x = Week, 
                          y = Day, 
                          fill = `USDM Class`),
                      color = "white",
                      linewidth = lw,
                      show.legend = TRUE) +
            scale_fill_manual(
              breaks = c("D0",
                         "D1",
                         "D2",
                         "D3",
                         "D4"),
              values = list(
                "None" = "gray90",
                "D0" = "#ffff00",
                "D1" = "#fcd37f",
                "D2" = "#ffaa00",
                "D3" = "#e60000",
                "D4" = "#730000"
              ),
              drop = FALSE,
              guide = 
                guide_legend(title = NULL,
                             nrow = 1,
                             override.aes = list(size = 0))
            ) + 
            geom_segment(data = month_breaks, 
                         mapping = aes(x = as.integer(Week) - 0.5, 
                                       y = 0.5,
                                       yend = 8 - as.integer(Day) + 0.5),
                         color = "black",
                         linewidth = lw - 0.3,
                         lineend = "round") +
            geom_segment(data = month_breaks, 
                         mapping = aes(x = as.integer(Week) - 0.5, 
                                       xend = as.integer(Week) + 0.5,
                                       y = 8 - as.integer(Day) + 0.5),
                         color = "black",
                         linewidth = lw - 0.3,
                         lineend = "round") +
            geom_segment(data = month_breaks, 
                         mapping = aes(x = as.integer(Week) + 0.5,
                                       y = 8 - as.integer(Day) + 0.5,
                                       yend = 7.5),
                         color = "black",
                         linewidth = lw - 0.3,
                         lineend = "round") +
            geom_rect(xmin = 0.5, xmax = 54.5, ymin = 0.5, ymax = 7.5,
                      color = "white",
                      fill = "transparent",
                      linewidth = lw) +
            scale_x_discrete(breaks = lab_breaks,
                             limits = 
                               factor(as.character(1:54),
                                      levels = 1:54, 
                                      labels = 1:54, 
                                      ordered = TRUE),
                             labels = month.abb,
                             position = "bottom",
                             expand = expansion(0,0),
                             drop = FALSE) +
            scale_y_discrete(
              name = "title",
              limits = rev(
                c("Sun", "Mon", "Tue",
                  "Wed", "Thu", "Fri",
                  "Sat")
              ),
              expand = expansion(0,0),
              drop = FALSE
            ) +
            coord_equal(clip = 'off') +
            # facet_wrap("Year", ncol = 1,
            #            strip.position = "bottom") +
            lemon::facet_rep_wrap("Year",
                                  ncol = 1,
                                  strip.position = "top",
                                  repeat.tick.labels = 'bottom') +
            theme_void() +
            theme(
              # axis.text.y = element_text(hjust = 1,
              #                            vjust = 0.5,
              #                            size = rel(0.7)),
              axis.text.y = element_blank(),
              axis.text.x = element_text(#face = "bold",
                hjust = 0.5,
                vjust = 0,
                size = rel(0.8)),
              legend.position = "bottom",
              legend.justification = "right",
              legend.text.position = "bottom",
              legend.text = element_text(face = "bold"),
              strip.text = element_text(face = "bold",
                                        hjust = 0,
                                        vjust = 0.5,
                                        size = rel(1.2)),
              plot.margin = margin(r = 0.5, unit = "in")
            )
          
          
          p %<>%
            ggplotGrob()
          
          left_axis <-
            p$layout %>%
            tibble::as_tibble() %>%
            dplyr::filter(name == "axis-l-1-1") %$%
            l
          
          right_axis <-
            p$layout %>%
            tibble::as_tibble() %>%
            dplyr::filter(name == "axis-r-1-1") %$%
            r
          
          guide_t <- 
            p$layout %>%
            tibble::as_tibble() %>%
            dplyr::filter(name == "guide-box-bottom") %$%
            t
          
          guide_l <- 
            p$layout %>%
            tibble::as_tibble() %>%
            dplyr::filter(name == "guide-box-bottom") %$%
            l
          
          p$widths[left_axis] <- unit(0.5, "in")
          # p$widths[right_axis] <- unit(0.5, "in")
          
          p %>%
            gtable::gtable_add_grob(
              grid::textGrob(
                label =
                  "US Drought Monitor",
                just = c(0,0),
                x = unit(0, "npc"),
                # y = unit(guide_top * 9, "in"),
                y = unit(0.6, "npc"),
                gp = grid::gpar(fontface = "bold",
                                fontsize = 18)
              ),
              t = guide_t,
              l = guide_l,
              name = "product",
              clip = "off"
            ) %>%
            gtable::gtable_add_grob(
              grid::textGrob(
                label =
                  county_names %>%
                  dplyr::filter(FSA_CODE == county) %$%
                  paste0(County," County, ", State),
                just = c(0,0),
                x = unit(0, "npc"),
                # y = unit(guide_top * 9, "in"),
                y = unit(0.1, "npc"),
                gp = grid::gpar(fontface = "plain",
                                fontsize = 14)
              ),
              t = guide_t,
              l = guide_l,
              name = "county",
              clip = "off"
            ) %>%
            grid::grid.draw()
        }
      )
  }


plot_county <- 
  function(county, state, years = 2000:2024){
    cairo_pdf( filename = paste0(county, "_", state, ".pdf"),
               width = 10,
               height = 7.5,
               bg = "white",
               fallback_resolution = 600)
    county_names %>%
      dplyr::filter(County == county,
                    State == state) %$%
      county_drought_calendar(FSA_CODE,
                              years = years, 
                              width = 4)
    dev.off()
    
  }

# plot_county(county = "Missoula", state = "Montana")
plot_county(county = "Missoula", state = "Montana", years = 2001:2024)
plot_county(county = "Asotin", state = "Washington", years = 2017:2024)
plot_county(county = "Okanogan", state = "Washington", years = 2017:2024)
plot_county(county = "Douglas", state = "Washington", years = 2001:2024)
plot_county(county = "Lake", state = "Montana", years = 2017:2024)
plot_county("Powell", "Montana")
plot_county("Flathead", "Montana")
plot_county("Roosevelt", "Montana")
plot_county("Ravalli", "Montana", years = 2017:2024)
plot_county("Boise", "Idaho")
plot_county("Glacier", "Montana", years = 2001:2024)
plot_county("Carter", "Montana", years = 2001:2024)
plot_county("Fallon", "Montana", years = 2001:2024)
plot_county("Steuben", "New York", years = 2001:2024)


plot_county("McKinley", "New Mexico", years = 2001:2024)

get_fsa_grazing_periods <-
  function(county, state){
    county_names %>%
      dplyr::filter(County == county,
                    State == state) %>%
      dplyr::left_join(
        arrow::read_parquet(file.path("data-derived", "fsa-normal-grazing-periods.parquet"))
      ) %>%
      dplyr::arrange(`Grazing Period Start Date`, 
                     # `Grazing Period End Date`, 
                     `Crop Name`, 
                     `Type Name`)
  }


c("Glacier","Carter", "Fallon") %T>%
  purrr::walk(\(x) plot_county(county = x, state = "Montana")) %>%
  purrr::walk(\(x){
    get_fsa_grazing_periods(county = x, state = "Montana") %>%
      readr::write_csv(paste0(x, "_Montana.csv"))  
  })





get_fsa_grazing_periods(county = "Asotin", state = "Washington")
get_fsa_grazing_periods(county = "Okanogan", state = "Washington") %>%
  readr::write_csv("Okanogan_Washington.csv")

get_fsa_grazing_periods(county = "Douglas", state = "Washington") %>%
  readr::write_csv("Douglas_Washington.csv")

get_fsa_grazing_periods(county = "Missoula", state = "Montana") %>%
  readr::write_csv("Missoula_Montana.csv")

get_fsa_grazing_periods(county = "McKinley", state = "New Mexico") %>%
  readr::write_csv("McKinley_New Mexico.csv")

get_fsa_grazing_periods(county = "Steuben", state = "New York") %>%
  readr::write_csv("Steuben_New York.csv")


calendar_and_grazing <-
  function(county, state){
    plot_county(county, state, years = 2001:2024)
    get_fsa_grazing_periods(county = county, state = state) %>%
      readr::write_csv(paste0(county, "_", state,".csv"))
  }

calendar_and_grazing(county = "Steuben", state = "New York")
calendar_and_grazing(county = "Emery", state = "Utah")
calendar_and_grazing(county = "Carbon", state = "Utah")
calendar_and_grazing(county = "Cherry", state = "Nebraska")
calendar_and_grazing(county = "Tulsa", state = "Oklahoma")

# library(tidyverse)
# library(openxlsx2)
# options("openxlsx2.maxWidth" = 64)
# 
# wb_workbook(creator = "Kyle Bocinsky",
#             title = "Montana Drought Classes — 2023-08-08") %>%
#   wb_add_worksheet("Counties") %>%
#   wb_add_data_table("Counties", 
#                     x = usdm_counties %>%
#                       dplyr::filter(Date == max(Date)) %>%
#                       dplyr::left_join(county_names) %>%
#                       dplyr::filter(State == "Montana") %>%
#                       dplyr::arrange(dplyr::desc(`USDM Class`), County) %>%
#                       dplyr::select(County, `USDM Class`),
#                     table_style = "TableStyleLight1",
#                     na.strings = ""
#   ) %>%
#   wb_freeze_pane(first_row = TRUE) %>%
#   wb_add_cell_style(
#     dims = "A1:M7000",
#     wrap_text = "1"
#   ) %>%
#   wb_set_col_widths(cols = 1:13,
#                     widths = "auto") %>%
#   wb_save(file = "data-derived/Montana Drought Classes — 2023-08-08.xlsx")
