mod_overlap_ui <- function(id) {
  ns <- shiny::NS(id)
  tagList(
    fluidRow(
      column(
        4,
        selectInput(
          ns("vuln_metric"),
          "Vulnerability metric (SVI)",
          choices = c(
            "SVI Overall" = "svi_overall",
            "Theme 1" = "svi_theme1",
            "Theme 2" = "svi_theme2",
            "Theme 3" = "svi_theme3",
            "Theme 4" = "svi_theme4"
          ),
          selected = "svi_overall"
        ),
        selectInput(
          ns("exposure_metric"),
          "Exposure metric (AQS)",
          choices = c("PM2.5" = "pm25", "Ozone" = "ozone"),
          selected = "pm25"
        ),
        uiOutput(ns("year_ui")),
        checkboxInput(ns("color_by_region"), "Color scatter by Census region", FALSE)
      ),
      column(
        8,
        helpText("Equity framing: how exposure distributions vary across vulnerability."),
        helpText("Note: descriptive patterns; no causal interpretation.")
      )
    ),
    fluidRow(
      column(6, plotOutput(ns("scatter"), height = 320)),
      column(6, plotOutput(ns("boxplot"), height = 320))
    ),
    div(class = "spacer"),
    fluidRow(
      column(6, leafletOutput(ns("overlap_map"), height = 420)),
      column(6, tableOutput(ns("overlap_table")))
    )
  )
}

mod_overlap_server <- function(id,
                               geo_sf,
                               county_master,
                               aqs_county_year,
                               county_analytic,
                               active_fips5) {
  moduleServer(id, function(input, output, session) {
    output$year_ui <- renderUI({
      cy <- aqs_county_year()
      yrs <- sort(unique(as.integer(cy$year)))
      if (length(yrs) == 0) return(NULL)
      sliderInput(session$ns("year"), "AQS year", min = min(yrs), max = max(yrs), value = max(yrs), step = 1, sep = "")
    })

    overlap_year <- reactive({
      cy <- aqs_county_year()
      yrs <- sort(unique(as.integer(cy$year)))
      if (length(yrs) == 0) return(NA_integer_)
      if (is.null(input$year) || !is.finite(input$year)) return(max(yrs))
      as.integer(input$year)
    })

    overlap_df <- reactive({
      ca <- county_analytic()
      cy <- aqs_county_year()
      year_val <- overlap_year()

      exp_df <- cy |>
        filter(.data$year == year_val) |>
        transmute(
          fips5 = .data$fips5,
          pm25 = .data$pm25_mean_ugm3,
          ozone = .data$ozone_mean_ppb
        )

      ca |>
        select(
          fips5, label, state_abbr, region,
          svi_overall, svi_theme1, svi_theme2, svi_theme3, svi_theme4,
          asthma_prev, copd_prev
        ) |>
        left_join(exp_df, by = "fips5")
    })

    output$scatter <- renderPlot({
      df <- overlap_df()
      vuln <- input$vuln_metric
      exp <- input$exposure_metric

      x <- df[[vuln]]
      y <- if (exp == "pm25") df$pm25 else df$ozone

      plot_df <- df |>
        mutate(v = x, e = y) |>
        filter(is.finite(.data$v) & is.finite(.data$e))

      if (nrow(plot_df) == 0) return(NULL)

      if (isTRUE(input$color_by_region)) {
        ggplot(plot_df, aes(x = v, y = e, color = region)) +
          geom_point(alpha = 0.35, size = 1.2) +
          geom_smooth(method = "loess", se = TRUE, color = "#2c7fb8") +
          theme_minimal(base_size = 12) +
          labs(x = vuln, y = exp, title = "Exposure vs vulnerability")
      } else {
        ggplot(plot_df, aes(x = v, y = e)) +
          geom_point(alpha = 0.35, size = 1.2, color = "#636363") +
          geom_smooth(method = "loess", se = TRUE, color = "#2c7fb8") +
          theme_minimal(base_size = 12) +
          labs(x = vuln, y = exp, title = "Exposure vs vulnerability")
      }
    })

    output$boxplot <- renderPlot({
      df <- overlap_df()
      vuln <- input$vuln_metric
      exp <- input$exposure_metric

      v <- df[[vuln]]
      e <- if (exp == "pm25") df$pm25 else df$ozone

      plot_df <- df |>
        mutate(v = v, e = e) |>
        filter(is.finite(.data$v) & is.finite(.data$e))
      if (nrow(plot_df) == 0) return(NULL)

      qs <- quantile(plot_df$v, probs = seq(0, 1, by = 0.2), na.rm = TRUE, type = 7)
      plot_df$v_quint <- cut(plot_df$v, breaks = qs, include.lowest = TRUE, labels = paste0("Q", 1:5))

      ggplot(plot_df, aes(x = v_quint, y = e)) +
        geom_boxplot(fill = "#9ecae1", outlier.alpha = 0.25) +
        theme_minimal(base_size = 12) +
        labs(x = "Vulnerability quintile", y = exp, title = "Exposure by vulnerability quintile")
    })

    overlap_hotspots <- reactive({
      df <- overlap_df()
      vuln <- input$vuln_metric
      exp <- input$exposure_metric

      v <- df[[vuln]]
      e <- if (exp == "pm25") df$pm25 else df$ozone

      ok <- is.finite(v) & is.finite(e)
      if (!any(ok)) return(df[0, ])

      v_cut <- quantile(v[ok], 0.9, na.rm = TRUE, type = 7)
      e_cut <- quantile(e[ok], 0.9, na.rm = TRUE, type = 7)

      df |>
        mutate(v = v, e = e) |>
        filter(is.finite(.data$v) & is.finite(.data$e)) |>
        filter(.data$v >= v_cut & .data$e >= e_cut) |>
        arrange(desc(.data$e)) |>
        select(fips5, label, state_abbr, v, e, asthma_prev, copd_prev) |>
        head(200)
    })

    output$overlap_table <- renderTable({
      df <- overlap_hotspots()
      if (nrow(df) == 0) return(data.frame())
      df |>
        mutate(
          v = round(.data$v, 3),
          e = round(.data$e, 3),
          asthma_prev = round(.data$asthma_prev, 1),
          copd_prev = round(.data$copd_prev, 1)
        )
    }, striped = TRUE, hover = TRUE, bordered = TRUE)

    output$overlap_map <- renderLeaflet({
      geo <- geo_sf()
      req(nrow(geo) > 0)

      leaflet(geo, options = leafletOptions(preferCanvas = TRUE)) |>
        addTiles(
          urlTemplate = "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
          options = tileOptions(subdomains = c("a", "b", "c", "d")),
          attribution = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
        ) |>
        # Default: continental US (avoid world view).
        setView(lng = -98.35, lat = 39.5, zoom = 4) |>
        addPolygons(
          layerId = ~fips5,
          color = "#ffffff",
          weight = 0.2,
          fillColor = "#e6e6e6",
          fillOpacity = 0.6,
          label = ~label,
          highlightOptions = highlightOptions(weight = 2, color = "#000000", bringToFront = TRUE)
        )
    })

    observeEvent(overlap_hotspots(), {
      geo <- geo_sf()
      hot <- overlap_hotspots()
      ids <- hot$fips5
      hot_geo <- geo[geo$fips5 %in% ids, ]

      leafletProxy("overlap_map", session = session) |>
        clearGroup("hot") |>
        addPolygons(
          data = hot_geo,
          group = "hot",
          layerId = ~fips5,
          color = "#b30000",
          weight = 1,
          fillColor = "#fb6a4a",
          fillOpacity = 0.8
        )
    }, ignoreInit = TRUE)

    observeEvent(input$overlap_map_shape_click, {
      click <- input$overlap_map_shape_click
      if (is.null(click$id)) return()
      active_fips5(click$id)
    })
  })
}
