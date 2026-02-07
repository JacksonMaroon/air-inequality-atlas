mod_model_ui <- function(id) {
  ns <- shiny::NS(id)
  tagList(
    fluidRow(
      column(
        4,
        selectInput(ns("response"), "Response (PLACES)", choices = c("Asthma % (age-adjusted)" = "asthma", "COPD % (age-adjusted)" = "copd"), selected = "asthma"),
        checkboxGroupInput(
          ns("predictors"),
          "Predictors",
          choices = c("PM2.5" = "pm25", "Ozone" = "ozone", "SVI overall" = "svi", "Smoking" = "smoking", "Obesity" = "obesity"),
          selected = c("pm25", "ozone", "svi")
        ),
        checkboxInput(ns("state_fe"), "State fixed effects", TRUE),
        radioButtons(ns("model_type"), "Model type", choices = c("Linear (robust SE)" = "lm", "GAM (nonlinear PM2.5)" = "gam"), selected = "lm"),
        uiOutput(ns("year_ui")),
        tags$hr(),
        tags$h5("What-if (Associational)"),
        sliderInput(ns("d_pm25"), "Delta PM2.5 (ug/m3)", min = -5, max = 5, value = 0, step = 0.1),
        sliderInput(ns("d_ozone"), "Delta Ozone (ppb)", min = -10, max = 10, value = 0, step = 0.5),
        uiOutput(ns("whatif_out"))
      ),
      column(
        8,
        tags$div(
          class = "callout",
          tags$b("Mandatory disclaimer: "),
          "This is an ecological, cross-sectional association model. Coefficients are not causal effects. ",
          "PLACES outcomes are modeled estimates and should be interpreted with care."
        ),
        plotOutput(ns("coef_plot"), height = 320),
        verbatimTextOutput(ns("model_summary")),
        plotOutput(ns("gam_smooth"), height = 260)
      )
    )
  )
}

mod_model_server <- function(id,
                             county_master,
                             aqs_county_year,
                             places_county_path,
                             svi_county_path,
                             active_fips5) {
  moduleServer(id, function(input, output, session) {
    require_pkgs(c("dplyr", "arrow", "sandwich", "ggplot2", "mgcv"))

    places <- read_parquet(places_county_path)
    svi <- read_parquet(svi_county_path)

    robust_coeftable <- function(mod, vcov_mat) {
      est <- stats::coef(mod)
      se <- sqrt(diag(vcov_mat))
      tval <- est / se
      df_res <- stats::df.residual(mod)
      pval <- 2 * stats::pt(abs(tval), df = df_res, lower.tail = FALSE)
      out <- cbind(
        Estimate = est,
        `Robust SE` = se,
        `t value` = tval,
        `Pr(>|t|)` = pval
      )
      out
    }

    kfold_cv_rmse <- function(model_type, formula, data, response, k = 5L, seed = 123L) {
      n <- nrow(data)
      if (!is.finite(n) || n < 5) return(NA_real_)
      k <- as.integer(max(2, min(k, n)))

      set.seed(seed)
      folds <- sample(rep_len(seq_len(k), n))

      rmses <- rep(NA_real_, k)
      for (i in seq_len(k)) {
        train <- data[folds != i, , drop = FALSE]
        test <- data[folds == i, , drop = FALSE]
        if (nrow(test) == 0 || nrow(train) < 10) next

        mod <- tryCatch(
          {
            if (model_type == "gam") {
              mgcv::gam(formula, data = train, method = "REML")
            } else {
              stats::lm(formula, data = train)
            }
          },
          error = function(e) NULL
        )
        if (is.null(mod)) next

        pred <- tryCatch(stats::predict(mod, newdata = test), error = function(e) rep(NA_real_, nrow(test)))
        y <- test[[response]]
        ok <- is.finite(y) & is.finite(pred)
        if (!any(ok)) next
        rmses[[i]] <- sqrt(mean((y[ok] - pred[ok])^2))
      }

      rmses <- rmses[is.finite(rmses)]
      if (length(rmses) == 0) return(NA_real_)
      mean(rmses)
    }

    output$year_ui <- renderUI({
      cy <- aqs_county_year()
      yrs <- sort(unique(as.integer(cy$year)))
      sliderInput(session$ns("year"), "AQS year (predictors)", min = min(yrs), max = max(yrs), value = max(yrs), step = 1, sep = "")
    })

    model_df <- reactive({
      cy <- aqs_county_year()
      yr <- input$year
      req(!is.null(yr))
      exp <- cy |>
        filter(.data$year == as.integer(yr)) |>
        transmute(fips5 = .data$fips5, pm25 = .data$pm25_mean_ugm3, ozone = .data$ozone_mean_ppb)

      cm <- county_master()

      df <- cm |>
        select(fips5, state_abbr, label) |>
        left_join(exp, by = "fips5") |>
        left_join(places, by = "fips5") |>
        left_join(svi, by = "fips5")

      df
    })

    fitted <- reactive({
      df <- model_df()
      preds <- input$predictors
      if (is.null(preds) || length(preds) == 0) preds <- character(0)

      y <- if (input$response == "asthma") "asthma_prev" else "copd_prev"

      x_terms <- c()
      if ("pm25" %in% preds) x_terms <- c(x_terms, "pm25")
      if ("ozone" %in% preds) x_terms <- c(x_terms, "ozone")
      if ("svi" %in% preds) x_terms <- c(x_terms, "svi_overall")
      if ("smoking" %in% preds) x_terms <- c(x_terms, "smoking_prev")
      if ("obesity" %in% preds) x_terms <- c(x_terms, "obesity_prev")

      if (length(x_terms) == 0) {
        stop("Select at least one predictor.", call. = FALSE)
      }

      df2 <- df |>
        filter(is.finite(.data[[y]]))
      for (x in x_terms) df2 <- df2 |> filter(is.finite(.data[[x]]))

      if (isTRUE(input$state_fe)) {
        df2$state_abbr <- factor(df2$state_abbr)
      }

      if (input$model_type == "gam") {
        # Nonlinear smooth on PM2.5 if included; others linear.
        smooth_term <- if ("pm25" %in% preds) "s(pm25)" else NULL
        base_terms <- setdiff(x_terms, "pm25")
        rhs <- c(smooth_term, base_terms)
        if (isTRUE(input$state_fe)) rhs <- c(rhs, "state_abbr")
        f <- as.formula(paste(y, "~", paste(rhs, collapse = " + ")))
        mod <- mgcv::gam(f, data = df2, method = "REML")
        list(type = "gam", model = mod, data = df2, response = y, predictors = rhs, formula = f)
      } else {
        rhs <- x_terms
        if (isTRUE(input$state_fe)) rhs <- c(rhs, "state_abbr")
        f <- as.formula(paste(y, "~", paste(rhs, collapse = " + ")))
        mod <- stats::lm(f, data = df2)
        list(type = "lm", model = mod, data = df2, response = y, predictors = rhs, formula = f)
      }
    })

    metrics <- reactive({
      fit <- fitted()
      mod <- fit$model

      r2 <- NA_real_
      if (fit$type == "lm") {
        r2 <- tryCatch(summary(mod)$r.squared, error = function(e) NA_real_)
      } else {
        r2 <- tryCatch(summary(mod)$r.sq, error = function(e) NA_real_)
      }

      rmse_cv <- kfold_cv_rmse(
        model_type = fit$type,
        formula = fit$formula,
        data = fit$data,
        response = fit$response,
        k = 5L,
        seed = 123L
      )

      list(r2 = r2, rmse_cv = rmse_cv, n = nrow(fit$data))
    })

    output$model_summary <- renderPrint({
      fit <- fitted()
      mod <- fit$model
      m <- metrics()
      if (fit$type == "lm") {
        vc <- sandwich::vcovHC(mod, type = "HC1")
        print(robust_coeftable(mod, vc))
      } else {
        print(summary(mod))
      }
      cat("\nRows used:", m$n, "\n")
      cat("R^2:", ifelse(is.finite(m$r2), sprintf("%.3f", m$r2), "NA"), "\n")
      cat("5-fold CV RMSE:", ifelse(is.finite(m$rmse_cv), sprintf("%.3f", m$rmse_cv), "NA"), "\n")
    })

    output$coef_plot <- renderPlot({
      fit <- fitted()
      if (fit$type != "lm") return(NULL)

      mod <- fit$model
      vc <- sandwich::vcovHC(mod, type = "HC1")
      ct <- robust_coeftable(mod, vc)
      df <- data.frame(
        term = rownames(ct),
        estimate = ct[, "Estimate"],
        se = ct[, "Robust SE"],
        stringsAsFactors = FALSE
      )
      df <- df |> filter(term != "(Intercept)")
      df$lo <- df$estimate - 1.96 * df$se
      df$hi <- df$estimate + 1.96 * df$se

      ggplot(df, aes(x = reorder(term, estimate), y = estimate)) +
        geom_hline(yintercept = 0, color = "#999999") +
        geom_point(color = "#08519c") +
        geom_errorbar(aes(ymin = lo, ymax = hi), width = 0.2, color = "#08519c") +
        coord_flip() +
        theme_minimal(base_size = 12) +
        labs(x = NULL, y = "Coefficient (percentage points)", title = "Associational coefficients (robust 95% CI)")
    })

    output$gam_smooth <- renderPlot({
      fit <- fitted()
      if (fit$type != "gam") return(NULL)
      mod <- fit$model
      if (!any(grepl("s\\(pm25\\)", names(mod$smooth)))) return(NULL)
      plot(mod, select = 1, shade = TRUE, main = "GAM smooth: PM2.5", xlab = "PM2.5", ylab = "Partial effect")
    })

    output$whatif_out <- renderUI({
      fit <- fitted()
      if (fit$type != "lm") {
        return(tags$small("What-if is implemented for the linear model only."))
      }
      mod <- fit$model
      vc <- sandwich::vcovHC(mod, type = "HC1")
      co <- stats::coef(mod)

      dpm <- input$d_pm25
      doz <- input$d_ozone

      beta_pm <- if ("pm25" %in% names(co)) co[["pm25"]] else 0
      beta_oz <- if ("ozone" %in% names(co)) co[["ozone"]] else 0

      est <- dpm * beta_pm + doz * beta_oz

      v <- 0
      if ("pm25" %in% colnames(vc)) v <- v + (dpm^2) * vc["pm25", "pm25"]
      if ("ozone" %in% colnames(vc)) v <- v + (doz^2) * vc["ozone", "ozone"]
      if (all(c("pm25", "ozone") %in% colnames(vc))) v <- v + 2 * dpm * doz * vc["pm25", "ozone"]

      se <- sqrt(max(0, v))
      lo <- est - 1.96 * se
      hi <- est + 1.96 * se

      tags$div(
        tags$small(
          "Predicted change (pp): ",
          sprintf("%.2f (%.2f, %.2f)", est, lo, hi)
        )
      )
    })
  })
}
