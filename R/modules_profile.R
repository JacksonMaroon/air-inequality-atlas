mod_profile_ui <- function(id) {
  ns <- shiny::NS(id)
  tagList(
    fluidRow(
      column(
        4,
        radioButtons(
          ns("peer_group"),
          "Peer group",
          choices = c("National" = "national", "State" = "state", "Region" = "region"),
          selected = "national",
          inline = TRUE
        )
      ),
      column(
        8,
        uiOutput(ns("county_header"))
      )
    ),
    fluidRow(
      column(6, plotOutput(ns("pollution_trend"), height = 320)),
      column(6, plotOutput(ns("svi_themes"), height = 320))
    ),
    fluidRow(
      column(6, uiOutput(ns("health_cards"))),
      column(6, plotOutput(ns("peer_percentiles"), height = 280))
    )
  )
}

mod_profile_server <- function(id,
                               county_master,
                               aqs_county_year,
                               county_analytic,
                               active_fips5) {
  moduleServer(id, function(input, output, session) {
    selected_row <- reactive({
      f <- active_fips5()
      if (is.null(f) || nchar(f) != 5) return(NULL)
      ca <- county_analytic()
      ca |> filter(.data$fips5 == f) |> head(1)
    })

    output$county_header <- renderUI({
      row <- selected_row()
      if (is.null(row) || nrow(row) == 0) {
        return(tags$p(tags$em("Select a county on the map (National Atlas tab) or via the search box.")))
      }
      tags$h3(row$label[[1]])
    })

    output$pollution_trend <- renderPlot({
      row <- selected_row()
      if (is.null(row) || nrow(row) == 0) return(NULL)
      f <- row$fips5[[1]]
      cy <- aqs_county_year() |> filter(.data$fips5 == f) |> arrange(.data$year)
      if (nrow(cy) == 0) return(NULL)

      long <- bind_rows(
        cy |> transmute(year = .data$year, pollutant = "PM2.5", value = .data$pm25_mean_ugm3),
        cy |> transmute(year = .data$year, pollutant = "Ozone (ppb)", value = .data$ozone_mean_ppb)
      )

      ggplot(long, aes(x = year, y = value)) +
        geom_line(color = "#2c7fb8", linewidth = 0.8, na.rm = TRUE) +
        geom_point(color = "#2c7fb8", size = 1.6, na.rm = TRUE) +
        facet_wrap(~pollutant, scales = "free_y", ncol = 1) +
        theme_minimal(base_size = 12) +
        labs(x = NULL, y = NULL, title = "Pollution trends (AQS annual; county-year)")
    })

    output$svi_themes <- renderPlot({
      row <- selected_row()
      if (is.null(row) || nrow(row) == 0) return(NULL)

      df <- data.frame(
        theme = c("Overall", "Theme 1", "Theme 2", "Theme 3", "Theme 4"),
        value = c(
          row$svi_overall[[1]],
          row$svi_theme1[[1]],
          row$svi_theme2[[1]],
          row$svi_theme3[[1]],
          row$svi_theme4[[1]]
        )
      )

      ggplot(df, aes(x = theme, y = value)) +
        geom_col(fill = "#756bb1") +
        coord_cartesian(ylim = c(0, 1)) +
        theme_minimal(base_size = 12) +
        labs(x = NULL, y = "Percentile (0-1)", title = "SVI breakdown") +
        theme(axis.text.x = element_text(angle = 20, hjust = 1))
    })

    output$health_cards <- renderUI({
      row <- selected_row()
      if (is.null(row) || nrow(row) == 0) return(NULL)

      bslib::layout_columns(
        bslib::value_box(
          title = "Asthma (PLACES)",
          value = sprintf("%.1f%% (%.1f–%.1f)", row$asthma_prev[[1]], row$asthma_lcl[[1]], row$asthma_ucl[[1]]),
          showcase = NULL
        ),
        bslib::value_box(
          title = "COPD (PLACES)",
          value = sprintf("%.1f%% (%.1f–%.1f)", row$copd_prev[[1]], row$copd_lcl[[1]], row$copd_ucl[[1]]),
          showcase = NULL
        ),
        col_widths = c(6, 6)
      )
    })

    output$peer_percentiles <- renderPlot({
      row <- selected_row()
      if (is.null(row) || nrow(row) == 0) return(NULL)

      ca <- county_analytic()
      peer <- switch(
        input$peer_group,
        state = ca |> filter(.data$state_abbr == row$state_abbr[[1]]),
        region = ca |> filter(.data$region == row$region[[1]]),
        ca
      )

      metrics <- data.frame(
        metric = c("PM2.5 (anchor)", "Ozone (anchor)", "Asthma", "SVI", "CBI"),
        value = c(
          row$pm25_mean_ugm3_anchor[[1]],
          row$ozone_mean_ppb_anchor[[1]],
          row$asthma_prev[[1]],
          row$svi_overall[[1]],
          row$cbi[[1]]
        ),
        stringsAsFactors = FALSE
      )

      pct <- function(v, x) {
        v <- v[is.finite(v)]
        if (length(v) == 0 || !is.finite(x)) return(NA_real_)
        mean(v <= x) * 100
      }

      metrics$percentile <- c(
        pct(peer$pm25_mean_ugm3_anchor, metrics$value[1]),
        pct(peer$ozone_mean_ppb_anchor, metrics$value[2]),
        pct(peer$asthma_prev, metrics$value[3]),
        pct(peer$svi_overall, metrics$value[4]),
        pct(peer$cbi, metrics$value[5])
      )

      ggplot(metrics, aes(x = reorder(metric, percentile), y = percentile)) +
        geom_col(fill = "#31a354") +
        coord_flip() +
        theme_minimal(base_size = 12) +
        labs(x = NULL, y = "Percentile (0-100)", title = "Peer percentile ranks")
    })
  })
}

