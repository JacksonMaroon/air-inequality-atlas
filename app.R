suppressPackageStartupMessages({
  library(shiny)
  library(bslib)
  library(dplyr)
  library(arrow)
  library(leaflet)
  library(sf)
  library(ggplot2)
  library(DT)
  library(jsonlite)
})

source("R/helpers_io.R")
source("R/helpers_metrics.R")
source("R/modules_map.R")
source("R/modules_profile.R")
source("R/modules_overlap.R")
source("R/modules_model.R")

assert_project_root()

data_files <- list(
  geo = proj_path("data", "geo_county_simplified.rds"),
  county_master = proj_path("data", "county_master.parquet"),
  aqs = proj_path("data", "aqs_county_year.parquet"),
  analytic = proj_path("data", "county_analytic.parquet"),
  dictionary = proj_path("data", "data_dictionary.parquet"),
  vintages = proj_path("data", "vintages.json")
)

missing <- names(data_files)[!vapply(data_files, file.exists, logical(1))]

if (length(missing) == 0) {
  geo_sf <- readRDS(data_files$geo)
  county_master <- read_parquet(data_files$county_master)
  aqs_county_year <- read_parquet(data_files$aqs)
  county_analytic_raw <- read_parquet(data_files$analytic)
  # county_analytic.parquet has a fixed schema; join display label for UI use.
  county_analytic <- county_analytic_raw |>
    left_join(county_master |> select(fips5, label), by = "fips5")
  data_dictionary <- read_parquet(data_files$dictionary)
  vintages <- jsonlite::read_json(data_files$vintages, simplifyVector = TRUE)
} else {
  geo_sf <- NULL
  county_master <- NULL
  aqs_county_year <- NULL
  county_analytic_raw <- NULL
  county_analytic <- NULL
  data_dictionary <- NULL
  vintages <- NULL
}

metric_choices <- c(
  "CBI (Compound Burden Index)" = "cbi",
  "PM2.5 (annual mean)" = "pm25",
  "Ozone (annual mean)" = "ozone",
  "Asthma (PLACES age-adjusted %)" = "asthma",
  "COPD (PLACES age-adjusted %)" = "copd",
  "SVI Overall (percentile)" = "svi"
)

ui <- bslib::page_navbar(
  title = "Air Inequality Atlas",
  header = tagList(
    tags$head(
      tags$link(rel = "stylesheet", type = "text/css", href = "styles.css")
    )
  ),
  sidebar = bslib::sidebar(
    width = 340,
    shiny::helpText("County-level U.S. atlas: pollution, health burden, and vulnerability."),
    shiny::helpText("All results are descriptive/associational (non-causal)."),
    selectInput("state_filter", "State filter", choices = c("All (US)" = "ALL"), selected = "ALL"),
    selectInput("metric", "Metric", choices = metric_choices, selected = "cbi"),
    uiOutput("year_ui"),
    selectizeInput(
      "county_search",
      "County search",
      choices = NULL,
      selected = NULL,
      options = list(placeholder = "Type a county name or FIPS...")
    )
  ),
  nav_panel(
    "Overview",
    div(
      class = "tab-pad",
      uiOutput("overview_valueboxes"),
      div(class = "spacer"),
      fluidRow(
        column(
          6,
          leafletOutput("overview_map", height = 420),
          uiOutput("overview_active_county_note")
        ),
        column(6, DTOutput("overview_top_table"))
      )
    )
  ),
  nav_panel(
    "National Atlas",
    div(
      class = "tab-pad",
      mod_atlas_ui("atlas")
    )
  ),
  nav_panel(
    "County Profile",
    div(
      class = "tab-pad",
      mod_profile_ui("profile")
    )
  ),
  nav_panel(
    "Inequality & Overlap",
    div(
      class = "tab-pad",
      mod_overlap_ui("overlap")
    )
  ),
  nav_panel(
    "Model Lab",
    div(
      class = "tab-pad",
      mod_model_ui("model")
    )
  ),
  nav_panel(
    "Data & Methods",
    div(
      class = "tab-pad",
      h3("Data & Methods"),
      uiOutput("methods_vintages"),
      div(class = "spacer"),
      h4("Limitations (Must Read)"),
      tags$ul(
        tags$li("AQS is not real-time air quality; monitoring coverage is uneven across counties."),
        tags$li("PLACES estimates are modeled and not intended for assessing county trends over time."),
        tags$li("SVI percentiles are not directly comparable across years; this app uses a single SVI year (2022).")
      ),
      div(class = "spacer"),
      h4("Download"),
      downloadButton("download_analytic_csv", "Download county_analytic.csv"),
      div(class = "spacer"),
      h4("Data Dictionary"),
      DTOutput("data_dictionary_table")
    )
  )
)

server <- function(input, output, session) {
  if (length(missing) > 0) {
    showModal(modalDialog(
      title = "Missing Data Outputs",
      easyClose = TRUE,
      footer = modalButton("OK"),
      tags$p("Required data files are missing:"),
      tags$ul(lapply(missing, function(x) tags$li(x))),
      tags$p("Run the ETL pipeline from the project root:"),
      tags$pre("cd /Users/jacksonmaroon/air-inequality-atlas\nRscript data-raw/run_all.R")
    ))
  } else {
    # Populate state filter + county search choices.
    states <- county_master |>
      distinct(state_abbr) |>
      arrange(state_abbr) |>
      pull(state_abbr)
    updateSelectInput(session, "state_filter", choices = c("All (US)" = "ALL", setNames(states, states)))

    choices_df <- county_master |>
      arrange(label) |>
      transmute(label = label, fips5 = fips5)
    choices <- stats::setNames(choices_df$fips5, choices_df$label)
    updateSelectizeInput(session, "county_search", choices = choices, server = TRUE)
  }

  active_fips5 <- reactiveVal(NULL)

  observeEvent(input$state_filter, {
    req(length(missing) == 0)

    cm <- county_master
    if (!is.null(input$state_filter) && input$state_filter != "ALL") {
      cm <- cm |> dplyr::filter(.data$state_abbr == input$state_filter)
    }

    choices_df <- cm |>
      arrange(label) |>
      transmute(label = label, fips5 = fips5)
    choices <- stats::setNames(choices_df$fips5, choices_df$label)

    sel <- input$county_search
    if (!is.null(sel) && sel %in% choices_df$fips5) {
      updateSelectizeInput(session, "county_search", choices = choices, selected = sel, server = TRUE)
    } else {
      updateSelectizeInput(session, "county_search", choices = choices, selected = NULL, server = TRUE)
    }
  }, ignoreInit = TRUE)

  observeEvent(input$county_search, {
    if (is.null(input$county_search) || input$county_search == "") return()
    active_fips5(input$county_search)
  }, ignoreInit = TRUE)

  output$year_ui <- renderUI({
    if (length(missing) > 0) return(NULL)
    metric <- input$metric
    if (!metric %in% c("pm25", "ozone")) return(NULL)

    yrs <- sort(unique(as.integer(aqs_county_year$year)))
    sliderInput("aqs_year", "AQS year", min = min(yrs), max = max(yrs), value = max(yrs), step = 1, sep = "")
  })

  filtered_counties <- reactive({
    req(length(missing) == 0)
    if (input$state_filter == "ALL") return(county_master)
    county_master |> filter(.data$state_abbr == input$state_filter)
  })

  filtered_geo <- reactive({
    req(length(missing) == 0)
    if (input$state_filter == "ALL") return(geo_sf)
    geo_sf |>
      dplyr::filter(.data$state_abbr == input$state_filter)
  })

  filtered_analytic <- reactive({
    req(length(missing) == 0)
    if (input$state_filter == "ALL") return(county_analytic)
    county_analytic |> filter(.data$state_abbr == input$state_filter)
  })

  # --- Overview tab ---
  output$overview_valueboxes <- renderUI({
    req(length(missing) == 0)
    df <- filtered_analytic()

    mk <- input$metric
    if (is.null(mk) || mk == "") mk <- "cbi"

    # Active county (if any)
    f <- active_fips5()
    active_label <- NA_character_
    if (!is.null(f) && nchar(f) == 5) {
      row <- county_master |>
        filter(.data$fips5 == f) |>
        head(1)
      if (nrow(row) > 0) active_label <- row$label[[1]]
    }

    # Metric-specific summary values.
    metric_name <- switch(
      mk,
      cbi = "CBI",
      pm25 = "PM2.5",
      ozone = "Ozone",
      asthma = "Asthma",
      copd = "COPD",
      svi = "SVI",
      mk
    )

    metric_units <- switch(
      mk,
      pm25 = "ug/m3",
      ozone = "ppb",
      asthma = "%",
      copd = "%",
      svi = "(0-1)",
      cbi = "",
      ""
    )

    values <- rep(NA_real_, nrow(df))
    year_sel <- input$aqs_year

    if (mk %in% c("pm25", "ozone")) {
      mv <- aqs_pollution_values_for_year(aqs_county_year, year_sel)
      joined <- df |>
        select(fips5) |>
        left_join(mv, by = "fips5")
      values <- if (mk == "pm25") joined$pm25_mean_ugm3 else joined$ozone_mean_ppb
    } else if (mk == "cbi") {
      values <- df$cbi
    } else if (mk == "asthma") {
      values <- df$asthma_prev
    } else if (mk == "copd") {
      values <- df$copd_prev
    } else if (mk == "svi") {
      values <- df$svi_overall
    }

    coverage_n <- sum(is.finite(values))
    mean_val <- if (coverage_n > 0) mean(values, na.rm = TRUE) else NA_real_

    active_val <- NA_real_
    if (!is.null(f) && nchar(f) == 5) {
      if (mk %in% c("pm25", "ozone")) {
        mv <- aqs_pollution_values_for_year(aqs_county_year, year_sel)
        row <- mv |>
          filter(.data$fips5 == f) |>
          head(1)
        if (nrow(row) > 0) {
          active_val <- if (mk == "pm25") row$pm25_mean_ugm3[[1]] else row$ozone_mean_ppb[[1]]
        }
      } else {
        row <- county_analytic |>
          filter(.data$fips5 == f) |>
          head(1)
        if (nrow(row) > 0) {
          active_val <- switch(
            mk,
            cbi = row$cbi[[1]],
            asthma = row$asthma_prev[[1]],
            copd = row$copd_prev[[1]],
            svi = row$svi_overall[[1]],
            NA_real_
          )
        }
      }
    }

    bslib::layout_columns(
      bslib::value_box(
        title = "Counties Shown",
        value = nrow(df),
        showcase = NULL
      ),
      bslib::value_box(
        title = paste0(metric_name, " Coverage"),
        value = coverage_n,
        showcase = NULL
      ),
      bslib::value_box(
        title = paste0("Mean ", metric_name),
        value = if (is.finite(mean_val)) {
          if (metric_units == "%") sprintf("%.1f%%", mean_val) else if (metric_units == "(0-1)") sprintf("%.2f", mean_val) else if (metric_units == "") sprintf("%.2f", mean_val) else sprintf("%.2f %s", mean_val, metric_units)
        } else {
          "NA"
        },
        showcase = NULL
      ),
      bslib::value_box(
        title = if (!is.na(active_label)) paste0("Active: ", active_label) else "Active County",
        value = if (is.finite(active_val)) {
          if (metric_units == "%") sprintf("%.1f%%", active_val) else if (metric_units == "(0-1)") sprintf("%.2f", active_val) else if (metric_units == "") sprintf("%.2f", active_val) else sprintf("%.2f %s", active_val, metric_units)
        } else {
          "None"
        },
        showcase = NULL
      ),
      col_widths = c(3, 3, 3, 3)
    )
  })

  output$overview_top_table <- renderDT({
    req(length(missing) == 0)
    df <- filtered_analytic() |>
      filter(is.finite(.data$cbi)) |>
      arrange(desc(.data$cbi)) |>
      transmute(
        County = .data$label,
        `PM2.5 (anchor)` = .data$pm25_mean_ugm3_anchor,
        `Ozone (anchor)` = .data$ozone_mean_ppb_anchor,
        `Asthma %` = .data$asthma_prev,
        `SVI` = .data$svi_overall,
        `CBI` = .data$cbi
      )

    DT::datatable(df, rownames = FALSE, options = list(pageLength = 25, scrollX = TRUE))
  })

  output$overview_map <- renderLeaflet({
    req(length(missing) == 0)
    leaflet(options = leafletOptions(preferCanvas = TRUE)) |>
      addProviderTiles("CartoDB.Positron") |>
      # Default view: continental US (avoid an initial world view)
      setView(lng = -98.35, lat = 39.5, zoom = 4)
  })

  observeEvent(input$overview_map_shape_click, {
    click <- input$overview_map_shape_click
    if (is.null(click$id)) return()
    active_fips5(click$id)
  })

  output$overview_active_county_note <- renderUI({
    req(length(missing) == 0)
    f <- active_fips5()
    if (is.null(f) || nchar(f) != 5) return(NULL)
    row <- county_master |>
      filter(.data$fips5 == f) |>
      head(1)
    if (nrow(row) == 0) return(NULL)
    tags$p(tags$b("Active county:"), row$label[[1]])
  })

  update_overview_active_outline <- function() {
    f <- active_fips5()
    proxy <- leaflet::leafletProxy("overview_map", session = session) |>
      leaflet::clearGroup("active")
    if (is.null(f) || nchar(f) != 5) return(proxy)

    geo <- filtered_geo()
    active_geo <- geo |>
      dplyr::filter(.data$fips5 == f)
    if (nrow(active_geo) == 0) return(proxy)

    proxy |>
      leaflet::addPolygons(
        data = active_geo,
        group = "active",
        layerId = ~fips5,
        color = "#000000",
        weight = 2.5,
        fillOpacity = 0,
        fill = FALSE
      )
  }

  zoom_overview_to_active <- function() {
    f <- active_fips5()
    if (is.null(f) || nchar(f) != 5) return(invisible(NULL))

    geo <- filtered_geo()
    active_geo <- geo |>
      dplyr::filter(.data$fips5 == f)
    if (nrow(active_geo) == 0) return(invisible(NULL))

    bb <- sf::st_bbox(active_geo)
    leaflet_fit_bounds_safe(leaflet::leafletProxy("overview_map", session = session), bb)
    invisible(NULL)
  }

  update_overview_map <- function() {
    geo <- filtered_geo()
    req(nrow(geo) > 0)

    proxy <- leaflet::leafletProxy("overview_map", session = session) |>
      leaflet::clearControls() |>
      leaflet::clearShapes()

    mk <- input$metric
    if (is.null(mk) || mk == "") mk <- "cbi"

    joined <- geo
    legend_pal <- NULL
    legend_vals <- NULL
    legend_title <- NULL

    an <- filtered_analytic()

    if (mk == "cbi") {
      joined <- joined |>
        dplyr::left_join(an |>
          dplyr::select(fips5, cbi_decile),
        by = "fips5")

      values <- joined$cbi_decile
      pal <- leaflet::colorBin(
        palette = viridisLite::viridis(10),
        domain = values,
        bins = 10,
        na.color = "#d9d9d9"
      )
      joined$fill <- pal(values)
      joined$tooltip <- paste0(joined$label, ": CBI decile ", ifelse(is.na(values), "NA", values))
      legend_pal <- pal
      legend_vals <- values
      legend_title <- "CBI decile"
    } else if (mk == "asthma") {
      joined <- joined |>
        dplyr::left_join(an |>
          dplyr::select(fips5, asthma_prev),
        by = "fips5")

      values <- joined$asthma_prev
      pal <- leaflet::colorBin(viridisLite::viridis(7), domain = values, bins = 7, na.color = "#d9d9d9")
      joined$fill <- pal(values)
      joined$tooltip <- paste0(joined$label, ": Asthma ", ifelse(is.na(values), "NA", sprintf("%.1f%%", values)))
      legend_pal <- pal
      legend_vals <- values
      legend_title <- "Asthma (%)"
    } else if (mk == "copd") {
      joined <- joined |>
        dplyr::left_join(an |>
          dplyr::select(fips5, copd_prev),
        by = "fips5")

      values <- joined$copd_prev
      pal <- leaflet::colorBin(viridisLite::viridis(7), domain = values, bins = 7, na.color = "#d9d9d9")
      joined$fill <- pal(values)
      joined$tooltip <- paste0(joined$label, ": COPD ", ifelse(is.na(values), "NA", sprintf("%.1f%%", values)))
      legend_pal <- pal
      legend_vals <- values
      legend_title <- "COPD (%)"
    } else if (mk == "svi") {
      joined <- joined |>
        dplyr::left_join(an |>
          dplyr::select(fips5, svi_overall),
        by = "fips5")

      values <- joined$svi_overall
      pal <- leaflet::colorBin(viridisLite::viridis(7), domain = values, bins = 7, na.color = "#d9d9d9")
      joined$fill <- pal(values)
      joined$tooltip <- paste0(joined$label, ": SVI ", ifelse(is.na(values), "NA", sprintf("%.2f", values)))
      legend_pal <- pal
      legend_vals <- values
      legend_title <- "SVI (0-1)"
    } else if (mk %in% c("pm25", "ozone")) {
      year_sel <- input$aqs_year
      mv <- aqs_pollution_values_for_year(aqs_county_year, year_sel)

      joined <- joined |>
        dplyr::left_join(mv, by = "fips5")

      if (mk == "pm25") {
        values <- joined$pm25_mean_ugm3
        pal <- leaflet::colorBin(viridisLite::viridis(7), domain = values, bins = 7, na.color = "#d9d9d9")
        joined$fill <- pal(values)
        joined$tooltip <- paste0(
          joined$label, ": PM2.5 ",
          ifelse(is.na(values), "NA", sprintf("%.2f", values)),
          if (!is.null(year_sel) && is.finite(year_sel)) paste0(" (", as.integer(year_sel), ")") else ""
        )
        legend_pal <- pal
        legend_vals <- values
        legend_title <- if (!is.null(year_sel) && is.finite(year_sel)) paste0("PM2.5 (", as.integer(year_sel), ")") else "PM2.5"
      } else {
        values <- joined$ozone_mean_ppb
        pal <- leaflet::colorBin(viridisLite::viridis(7), domain = values, bins = 7, na.color = "#d9d9d9")
        joined$fill <- pal(values)
        joined$tooltip <- paste0(
          joined$label, ": Ozone ",
          ifelse(is.na(values), "NA", sprintf("%.2f", values)),
          " ppb",
          if (!is.null(year_sel) && is.finite(year_sel)) paste0(" (", as.integer(year_sel), ")") else ""
        )
        legend_pal <- pal
        legend_vals <- values
        legend_title <- if (!is.null(year_sel) && is.finite(year_sel)) paste0("Ozone ppb (", as.integer(year_sel), ")") else "Ozone (ppb)"
      }
    } else {
      joined$fill <- "#d9d9d9"
      joined$tooltip <- joined$label
    }

    labels <- lapply(joined$tooltip, htmltools::HTML)
    proxy <- proxy |>
      leaflet::addPolygons(
        data = joined,
        layerId = ~fips5,
        color = "#ffffff",
        weight = 0.25,
        fillColor = ~fill,
        fillOpacity = 0.85,
        label = labels,
        highlightOptions = leaflet::highlightOptions(weight = 2, color = "#000000", bringToFront = TRUE)
      )

    if (!is.null(legend_pal)) {
      proxy <- proxy |>
        leaflet::addLegend("bottomright", pal = legend_pal, values = legend_vals, title = legend_title)
    }

    if (input$state_filter == "ALL") {
      proxy <- proxy |> leaflet::setView(lng = -98.35, lat = 39.5, zoom = 4)
    } else {
      proxy <- leaflet_fit_bounds_safe(proxy, sf::st_bbox(geo))
    }

    update_overview_active_outline()
    proxy
  }

  # Initial draw after first flush so the Leaflet widget exists on the client.
  session$onFlushed(function() {
    update_overview_map()
  }, once = TRUE)

  observeEvent(
    {
      list(input$metric, input$aqs_year, input$state_filter)
    },
    update_overview_map,
    ignoreInit = TRUE
  )

  observeEvent(active_fips5(), {
    update_overview_active_outline()
    zoom_overview_to_active()
  }, ignoreInit = TRUE)

  # --- Modules ---
  mod_atlas_server(
    "atlas",
    geo_sf = filtered_geo,
    county_master = filtered_counties,
    aqs_county_year = reactive(aqs_county_year),
    county_analytic = filtered_analytic,
    metric = reactive(input$metric),
    aqs_year = reactive(input$aqs_year),
    active_fips5 = active_fips5
  )

  mod_profile_server(
    "profile",
    county_master = reactive(county_master),
    aqs_county_year = reactive(aqs_county_year),
    county_analytic = reactive(county_analytic),
    active_fips5 = active_fips5
  )

  mod_overlap_server(
    "overlap",
    geo_sf = reactive(geo_sf),
    county_master = reactive(county_master),
    aqs_county_year = reactive(aqs_county_year),
    county_analytic = reactive(county_analytic),
    active_fips5 = active_fips5
  )

  mod_model_server(
    "model",
    county_master = reactive(county_master),
    aqs_county_year = reactive(aqs_county_year),
    places_county_path = proj_path("data", "places_county.parquet"),
    svi_county_path = proj_path("data", "svi_county_2022.parquet"),
    active_fips5 = active_fips5
  )

  # --- Data & Methods tab ---
  output$data_dictionary_table <- renderDT({
    req(length(missing) == 0)
    DT::datatable(data_dictionary, rownames = FALSE, options = list(pageLength = 25, scrollX = TRUE))
  })

  output$methods_vintages <- renderUI({
    req(length(missing) == 0)
    tagList(
      h4("Vintages"),
      tags$pre(paste(capture.output(str(vintages, max.level = 3)), collapse = "\n"))
    )
  })

  output$download_analytic_csv <- downloadHandler(
    filename = function() "county_analytic.csv",
    content = function(file) {
      req(length(missing) == 0)
      df <- as.data.frame(county_analytic_raw)
      utils::write.csv(df, file, row.names = FALSE)
    }
  )
}

shinyApp(ui, server)
