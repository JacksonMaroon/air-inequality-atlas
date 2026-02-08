mod_atlas_ui <- function(id) {
  ns <- shiny::NS(id)

  tagList(
    fluidRow(
      column(
        4,
        checkboxInput(ns("bivar_mode"), "Bivariate map mode (3x3)", FALSE),
        uiOutput(ns("bivar_ui"))
      ),
      column(
        8,
        helpText("Tip: click a county to set the active county used in the County Profile tab.")
      )
    ),
    leafletOutput(ns("map"), height = 680),
    uiOutput(ns("active_county_note"))
  )
}

mod_atlas_server <- function(id,
                            geo_sf,
                            county_master,
                            aqs_county_year,
                            county_analytic,
                            metric,
                            aqs_year,
                            active_fips5) {
  moduleServer(id, function(input, output, session) {
    metric_label <- function(key) {
      switch(
        key,
        pm25 = "PM2.5",
        ozone = "Ozone",
        asthma = "Asthma",
        svi = "SVI overall",
        cbi = "CBI",
        key
      )
    }

    output$bivar_ui <- renderUI({
      if (!isTRUE(input$bivar_mode)) return(NULL)
      tagList(
        selectInput(
          session$ns("bivar_x"),
          "X metric",
          choices = c("PM2.5" = "pm25", "Ozone" = "ozone", "Asthma" = "asthma", "SVI overall" = "svi"),
          selected = "pm25"
        ),
        selectInput(
          session$ns("bivar_y"),
          "Y metric",
          choices = c("Asthma" = "asthma", "SVI overall" = "svi", "PM2.5" = "pm25", "Ozone" = "ozone"),
          selected = "asthma"
        )
      )
    })

    # Bivariate palette (x increases left->right, y increases bottom->top)
    bivar_palette <- function() {
      matrix(
        c(
          "#e8e8e8", "#ace4e4", "#5ac8c8",
          "#dfb0d6", "#a5add3", "#5698b9",
          "#be64ac", "#8c62aa", "#3b4994"
        ),
        nrow = 3,
        byrow = TRUE
      )
    }

    bivar_legend_html <- function(xlab, ylab) {
      cols <- bivar_palette()
      cell <- function(col) {
        shiny::tags$td(style = paste0("background:", col, "; width:18px; height:18px;"))
      }
      shiny::tags$div(
        style = "background: rgba(255,255,255,0.92); padding: 8px 10px; border: 1px solid #ddd; border-radius: 6px; font-size: 12px;",
        shiny::tags$div(shiny::tags$b("Bivariate: "), xlab, " vs ", ylab),
        shiny::tags$table(
          style = "border-collapse: collapse; margin-top: 6px;",
          shiny::tags$tr(cell(cols[3, 1]), cell(cols[3, 2]), cell(cols[3, 3]), shiny::tags$td(style = "padding-left:6px;", "High ", ylab)),
          shiny::tags$tr(cell(cols[2, 1]), cell(cols[2, 2]), cell(cols[2, 3])),
          shiny::tags$tr(cell(cols[1, 1]), cell(cols[1, 2]), cell(cols[1, 3]), shiny::tags$td(style = "padding-left:6px;", "Low ", ylab)),
          shiny::tags$tr(shiny::tags$td(colspan = 3, style = "padding-top:4px;", shiny::tags$span("Low ", xlab, " -> High ", xlab)))
        )
      )
    }

    render_base_map <- function() {
      leaflet::leaflet(options = leaflet::leafletOptions(preferCanvas = TRUE)) |>
        leaflet::addTiles(
          urlTemplate = "https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png",
          options = leaflet::tileOptions(subdomains = c("a", "b", "c", "d")),
          attribution = '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>'
        ) |>
        # Default view: continental US (avoids an initial world view)
        leaflet::setView(lng = -98.35, lat = 39.5, zoom = 4)
    }

    output$map <- renderLeaflet({
      geo <- geo_sf()
      req(nrow(geo) > 0)
      m <- render_base_map()

      if (isTRUE(input$bivar_mode)) {
        xk <- input$bivar_x
        yk <- input$bivar_y
        if (is.null(xk) || xk == "") xk <- "pm25"
        if (is.null(yk) || yk == "") yk <- "asthma"

        year_val <- aqs_year()
        mv <- aqs_pollution_values_for_year(aqs_county_year(), year_val)

        an <- county_analytic() |>
          dplyr::select(fips5, asthma_prev, svi_overall)

        joined <- geo |>
          dplyr::left_join(an, by = "fips5") |>
          dplyr::left_join(mv, by = "fips5")

        x <- dplyr::case_when(
          xk == "pm25" ~ joined$pm25_mean_ugm3,
          xk == "ozone" ~ joined$ozone_mean_ppb,
          xk == "asthma" ~ joined$asthma_prev,
          xk == "svi" ~ joined$svi_overall,
          TRUE ~ NA_real_
        )
        y <- dplyr::case_when(
          yk == "pm25" ~ joined$pm25_mean_ugm3,
          yk == "ozone" ~ joined$ozone_mean_ppb,
          yk == "asthma" ~ joined$asthma_prev,
          yk == "svi" ~ joined$svi_overall,
          TRUE ~ NA_real_
        )

        cls <- bivar_tercile_class(x, y)

        cols <- bivar_palette()
        col_map <- setNames(
          as.vector(t(cols)),
          as.vector(outer(1:3, 1:3, function(a, b) paste0(a, "-", b)))
        )

        joined$fill <- ifelse(is.na(cls), "#d9d9d9", unname(col_map[as.character(cls)]))

        m <- m |>
          leaflet::addPolygons(
            data = joined,
            layerId = ~fips5,
            color = "#ffffff",
            weight = 0.25,
            fillColor = ~fill,
            fillOpacity = 0.85,
            label = ~label,
            highlightOptions = leaflet::highlightOptions(weight = 2, color = "#000000", bringToFront = TRUE)
          ) |>
          leaflet::addControl(
            html = as.character(bivar_legend_html(metric_label(xk), metric_label(yk))),
            position = "bottomleft"
          )
      } else {
        mk <- metric()
        year_val <- aqs_year()
        an <- county_analytic()

        joined <- geo
        legend_pal <- NULL
        legend_vals <- NULL
        legend_title <- NULL

        if (mk == "cbi") {
          joined <- joined |>
            dplyr::left_join(an |>
              dplyr::select(fips5, cbi, cbi_decile),
            by = "fips5")
          values <- joined$cbi
          pal <- leaflet::colorBin(viridisLite::viridis(7), domain = values, bins = 7, na.color = "#d9d9d9")
          joined$fill <- pal(values)
          joined$tooltip <- paste0(joined$label, ": CBI ", ifelse(is.na(joined$cbi), "NA", sprintf("%.2f", joined$cbi)))
          legend_pal <- pal
          legend_vals <- values
          legend_title <- "CBI"
        } else if (mk == "asthma") {
          joined <- joined |>
            dplyr::left_join(an |>
              dplyr::select(fips5, asthma_prev),
            by = "fips5")
          values <- joined$asthma_prev
          pal <- leaflet::colorBin(viridisLite::viridis(7), domain = values, bins = 7, na.color = "#d9d9d9")
          joined$fill <- pal(values)
          joined$tooltip <- paste0(joined$label, ": Asthma ", ifelse(is.na(joined$asthma_prev), "NA", sprintf("%.1f%%", joined$asthma_prev)))
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
          joined$tooltip <- paste0(joined$label, ": COPD ", ifelse(is.na(joined$copd_prev), "NA", sprintf("%.1f%%", joined$copd_prev)))
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
          joined$tooltip <- paste0(joined$label, ": SVI ", ifelse(is.na(joined$svi_overall), "NA", sprintf("%.2f", joined$svi_overall)))
          legend_pal <- pal
          legend_vals <- values
          legend_title <- "SVI (0-1)"
        } else if (mk %in% c("pm25", "ozone")) {
          cy <- aqs_county_year()
          year_sel <- year_val
          if (nrow(cy) == 0) {
            year_sel <- NA_integer_
          } else if (is.null(year_sel) || !is.finite(year_sel)) {
            year_sel <- max(as.integer(cy$year), na.rm = TRUE)
          } else {
            year_sel <- as.integer(year_sel)
          }

          mv <- aqs_pollution_values_for_year(cy, year_sel)

          joined <- joined |>
            dplyr::left_join(mv, by = "fips5")

          if (mk == "pm25") {
            values <- joined$pm25_mean_ugm3
            pal <- leaflet::colorBin(viridisLite::viridis(7), domain = values, bins = 7, na.color = "#d9d9d9")
            joined$fill <- pal(values)
            joined$tooltip <- paste0(
              joined$label, ": PM2.5 ",
              ifelse(is.na(joined$pm25_mean_ugm3), "NA", sprintf("%.2f", joined$pm25_mean_ugm3)),
              if (!is.na(year_sel)) paste0(" (", year_sel, ")") else ""
            )
            legend_pal <- pal
            legend_vals <- values
            legend_title <- if (!is.na(year_sel)) paste0("PM2.5 (", year_sel, ")") else "PM2.5"
          } else {
            values <- joined$ozone_mean_ppb
            pal <- leaflet::colorBin(viridisLite::viridis(7), domain = values, bins = 7, na.color = "#d9d9d9")
            joined$fill <- pal(values)
            joined$tooltip <- paste0(
              joined$label, ": Ozone ",
              ifelse(is.na(joined$ozone_mean_ppb), "NA", sprintf("%.2f", joined$ozone_mean_ppb)),
              " ppb",
              if (!is.na(year_sel)) paste0(" (", year_sel, ")") else ""
            )
            legend_pal <- pal
            legend_vals <- values
            legend_title <- if (!is.na(year_sel)) paste0("Ozone ppb (", year_sel, ")") else "Ozone (ppb)"
          }
        } else {
          joined$fill <- "#d9d9d9"
          joined$tooltip <- joined$label
        }

        labels <- lapply(joined$tooltip, htmltools::HTML)

        m <- m |>
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
          m <- m |>
            leaflet::addLegend("bottomright", pal = legend_pal, values = legend_vals, title = legend_title)
        }
      }

      # Redraw the active outline after any full re-render without forcing this
      # output to depend on active_fips5().
	      f <- shiny::isolate(active_fips5())
	      if (!is.null(f) && nchar(f) == 5) {
	        active_geo <- geo |>
	          dplyr::filter(.data$fips5 == f)
	        if (nrow(active_geo) > 0) {
	          m <- m |>
	            leaflet::addPolygons(
	              data = active_geo,
	              group = "active",
	              layerId = ~paste0("active_", fips5),
	              color = "#000000",
	              weight = 2.5,
	              fillOpacity = 0,
	              fill = FALSE
	            )
	        }
	      }

      # View: national vs state subset.
      bb <- sf::st_bbox(geo)
      if (nrow(geo) > 1000) {
        # National view: show continental US by default (AK/HI can be panned to).
        m <- m |>
          leaflet::setView(lng = -98.35, lat = 39.5, zoom = 4)
      } else {
        # State view: fit to the filtered geometry bounds.
        m <- leaflet_fit_bounds_safe(m, bb)
      }

      m
    })

	    update_active_outline <- function() {
	      f <- active_fips5()
	      proxy <- leaflet::leafletProxy("map", session = session) |>
	        leaflet::clearGroup("active")
      if (is.null(f) || nchar(f) != 5) return(proxy)

      geo <- geo_sf()
      active_geo <- geo |>
        dplyr::filter(.data$fips5 == f)
      if (nrow(active_geo) == 0) return(proxy)

	      proxy |>
	        leaflet::addPolygons(
	          data = active_geo,
	          group = "active",
	          layerId = ~paste0("active_", fips5),
	          color = "#000000",
	          weight = 2.5,
	          fillOpacity = 0,
	          fill = FALSE
	        )
	    }

    zoom_to_active <- function() {
      f <- active_fips5()
      if (is.null(f) || nchar(f) != 5) return(invisible(NULL))

      geo <- geo_sf()
      active_geo <- geo |>
        dplyr::filter(.data$fips5 == f)
      if (nrow(active_geo) == 0) return(invisible(NULL))

      bb <- sf::st_bbox(active_geo)
      leaflet_fit_bounds_safe(leaflet::leafletProxy("map", session = session), bb)
      invisible(NULL)
    }

	    observeEvent(input$map_shape_click, {
	      click <- input$map_shape_click
	      f <- extract_fips5(click$id)
	      if (is.null(f)) return()
	      active_fips5(f)
	    })

    observeEvent(active_fips5(), {
      update_active_outline()
      zoom_to_active()
    }, ignoreInit = TRUE)

    output$active_county_note <- renderUI({
      f <- active_fips5()
      if (is.null(f) || nchar(f) != 5) return(NULL)
      cm <- county_master()
      row <- cm |> dplyr::filter(.data$fips5 == f)
      if (nrow(row) == 0) return(NULL)
      shiny::tags$p(shiny::tags$b("Active county:"), row$label[[1]])
    })
  })
}
