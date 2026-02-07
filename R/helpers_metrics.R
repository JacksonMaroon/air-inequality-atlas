make_fips5 <- function(state_code, county_code) {
  state_code <- sprintf("%02d", as.integer(state_code))
  county_code <- sprintf("%03d", as.integer(county_code))
  paste0(state_code, county_code)
}

weighted_mean_na <- function(x, w) {
  if (length(x) != length(w)) stop("x and w must have same length", call. = FALSE)
  ok <- is.finite(x) & is.finite(w) & w > 0
  if (!any(ok)) return(NA_real_)
  sum(x[ok] * w[ok]) / sum(w[ok])
}

zscore <- function(x) {
  mu <- mean(x, na.rm = TRUE)
  sig <- stats::sd(x, na.rm = TRUE)
  if (!is.finite(sig) || sig == 0) return(rep(NA_real_, length(x)))
  (x - mu) / sig
}

cbi_compute <- function(z_pm25, z_ozone, z_asthma, z_svi) {
  ifelse(
    is.finite(z_pm25) & is.finite(z_ozone) & is.finite(z_asthma) & is.finite(z_svi),
    0.4 * z_pm25 + 0.2 * z_ozone + 0.2 * z_asthma + 0.2 * z_svi,
    NA_real_
  )
}

tercile_class <- function(x) {
  qs <- stats::quantile(x, probs = c(1 / 3, 2 / 3), na.rm = TRUE, names = FALSE, type = 7)
  out <- rep(NA_integer_, length(x))
  out[!is.na(x) & x <= qs[1]] <- 1L
  out[!is.na(x) & x > qs[1] & x <= qs[2]] <- 2L
  out[!is.na(x) & x > qs[2]] <- 3L
  out
}

bivar_tercile_class <- function(x, y) {
  xc <- tercile_class(x)
  yc <- tercile_class(y)
  out <- rep(NA_character_, length(x))
  ok <- !is.na(xc) & !is.na(yc)
  out[ok] <- paste0(xc[ok], "-", yc[ok])
  factor(out, levels = as.vector(outer(1:3, 1:3, function(a, b) paste0(a, "-", b))))
}

nearest_year <- function(target_year, years_available) {
  years_available <- sort(unique(as.integer(years_available)))
  target_year <- as.integer(target_year)
  if (length(years_available) == 0) stop("years_available is empty", call. = FALSE)
  diffs <- abs(years_available - target_year)
  best <- years_available[diffs == min(diffs)]
  max(best)
}

state_crosswalk <- function() {
  data <- data.frame(
    state_name = c(as.character(state.name), "District of Columbia"),
    state_abbr = c(as.character(state.abb), "DC"),
    stringsAsFactors = FALSE
  )
  # FIPS mapping (50 states + DC). Hard-coded to avoid extra dependencies.
  fips <- c(
    AL = "01", AK = "02", AZ = "04", AR = "05", CA = "06", CO = "08", CT = "09",
    DE = "10", DC = "11", FL = "12", GA = "13", HI = "15", ID = "16", IL = "17",
    IN = "18", IA = "19", KS = "20", KY = "21", LA = "22", ME = "23", MD = "24",
    MA = "25", MI = "26", MN = "27", MS = "28", MO = "29", MT = "30", NE = "31",
    NV = "32", NH = "33", NJ = "34", NM = "35", NY = "36", NC = "37", ND = "38",
    OH = "39", OK = "40", OR = "41", PA = "42", RI = "44", SC = "45", SD = "46",
    TN = "47", TX = "48", UT = "49", VT = "50", VA = "51", WA = "53", WV = "54",
    WI = "55", WY = "56"
  )
  data$state_fips2 <- unname(fips[data$state_abbr])

  # Census region/division from base datasets for 50 states; add DC manually.
  reg <- data.frame(
    state_abbr = state.abb,
    region = as.character(state.region),
    division = as.character(state.division),
    stringsAsFactors = FALSE
  )
  reg <- rbind(
    reg,
    data.frame(state_abbr = "DC", region = "South", division = "South Atlantic", stringsAsFactors = FALSE)
  )
  data <- merge(data, reg, by = "state_abbr", all.x = TRUE, sort = FALSE)
  data[order(data$state_fips2), c("state_fips2", "state_abbr", "state_name", "region", "division")]
}

