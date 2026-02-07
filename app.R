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
        column(6, leafletOutput("overview_map", height = 420)),
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
    bslib::layout_columns(
      bslib::value_box(
        title = "PM2.5 Coverage (Anchor)",
        value = sum(df$has_pm25_anchor %in% TRUE, na.rm = TRUE),
        showcase = NULL
      ),
      bslib::value_box(
        title = "Ozone Coverage (Anchor)",
        value = sum(df$has_ozone_anchor %in% TRUE, na.rm = TRUE),
        showcase = NULL
      ),
      bslib::value_box(
        title = "Mean Asthma (PLACES)",
        value = sprintf("%.1f%%", mean(df$asthma_prev, na.rm = TRUE)),
        showcase = NULL
      ),
      bslib::value_box(
        title = "Counties in Top CBI Decile",
        value = sum(df$cbi_decile == 10, na.rm = TRUE),
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
      ) |>
      head(25)

    DT::datatable(df, rownames = FALSE, options = list(pageLength = 25, scrollX = TRUE))
  })

  output$overview_map <- renderLeaflet({
    req(length(missing) == 0)
    pal <- colorBin(
      palette = viridisLite::viridis(10),
      domain = county_analytic$cbi_decile,
      bins = 10,
      na.color = "#d9d9d9"
    )

    geo <- filtered_geo()
    joined <- geo |>
      left_join(county_analytic |> select(fips5, cbi_decile), by = "fips5")

    m <- leaflet(joined, options = leafletOptions(preferCanvas = TRUE)) |>
      addProviderTiles("CartoDB.Positron")

    if (input$state_filter == "ALL") {
      # Default: continental US (avoid world view).
      m <- m |> setView(lng = -98.35, lat = 39.5, zoom = 4)
    } else {
      bb <- sf::st_bbox(joined)
      m <- m |> fitBounds(bb$xmin, bb$ymin, bb$xmax, bb$ymax)
    }

    m |>
      addPolygons(
        layerId = ~fips5,
        color = "#ffffff",
        weight = 0.3,
        fillColor = ~pal(cbi_decile),
        fillOpacity = 0.85,
        label = ~paste0(label, ": CBI decile ", ifelse(is.na(cbi_decile), "NA", cbi_decile)),
        highlightOptions = highlightOptions(weight = 2, color = "#000000", bringToFront = TRUE)
      ) |>
      addLegend("bottomright", pal = pal, values = ~cbi_decile, title = "CBI decile")
  })

  observeEvent(input$overview_map_shape_click, {
    click <- input$overview_map_shape_click
    if (is.null(click$id)) return()
    active_fips5(click$id)
  })

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
