find_project_root <- function() {
  dir <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)
  for (i in 1:10) {
    if (file.exists(file.path(dir, "R", "helpers_io.R"))) return(dir)
    dir <- dirname(dir)
  }
  stop("Unable to locate project root from: ", getwd(), call. = FALSE)
}

root <- find_project_root()
source(file.path(root, "R", "helpers_io.R"))

test_that("bbox_to_lnglat returns NULL for missing/invalid inputs", {
  expect_true(is.null(bbox_to_lnglat(NULL)))
  expect_true(is.null(bbox_to_lnglat(c(xmin = NA_real_, ymin = 0, xmax = 1, ymax = 1))))
  expect_true(is.null(bbox_to_lnglat(c(a = 1, b = 2, c = 3, d = 4))))
})

test_that("bbox_to_lnglat preserves standard lon/lat bbox", {
  bb <- c(xmin = -124.4, ymin = 32.5, xmax = -114.1, ymax = 42.0)
  out <- bbox_to_lnglat(bb)
  expect_equal(unname(out), c(-124.4, 32.5, -114.1, 42.0))
  expect_equal(names(out), c("lng1", "lat1", "lng2", "lat2"))
})

test_that("bbox_to_lnglat fixes swapped axis-order bboxes", {
  # Example: California bbox with axis-order confusion (lat/lng swapped).
  bb_swapped <- c(xmin = 32.5, ymin = -124.4, xmax = 42.0, ymax = -114.1)
  out <- bbox_to_lnglat(bb_swapped)
  expect_equal(unname(out), c(-124.4, 32.5, -114.1, 42.0))
})

test_that("bbox_to_lnglat prefers positive latitudes when both variants are valid", {
  # Example: eastern US bbox could still be 'valid' when swapped, but should
  # prefer positive-latitude interpretation for this app.
  bb_swapped <- c(xmin = 40.0, ymin = -79.0, xmax = 45.0, ymax = -71.0)
  out <- bbox_to_lnglat(bb_swapped)
  expect_equal(unname(out), c(-79.0, 40.0, -71.0, 45.0))
})

test_that("extract_fips5 returns 5-digit county fips from overlay layerIds", {
  expect_equal(extract_fips5("06037"), "06037")
  expect_equal(extract_fips5("active_27007"), "27007")
  expect_equal(extract_fips5("hot_55109"), "55109")
  expect_true(is.null(extract_fips5(NULL)))
  expect_true(is.null(extract_fips5("nope")))
})
