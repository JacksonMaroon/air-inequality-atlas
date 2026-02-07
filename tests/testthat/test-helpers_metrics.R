find_project_root <- function() {
  dir <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)
  for (i in 1:10) {
    if (file.exists(file.path(dir, "R", "helpers_metrics.R"))) return(dir)
    dir <- dirname(dir)
  }
  stop("Unable to locate project root from: ", getwd(), call. = FALSE)
}

root <- find_project_root()
source(file.path(root, "R", "helpers_metrics.R"))

test_that("make_fips5 pads state and county correctly", {
  expect_equal(make_fips5("1", "1"), "01001")
  expect_equal(make_fips5(6, 75), "06075")
})

test_that("weighted_mean_na handles NA and zero weights", {
  x <- c(1, 2, 3, NA)
  w <- c(1, 1, 0, 10)
  expect_equal(weighted_mean_na(x, w), 1.5)
  expect_true(is.na(weighted_mean_na(c(NA, NA), c(1, 1))))
})

test_that("zscore returns NA when sd is zero", {
  z <- zscore(rep(5, 10))
  expect_true(all(is.na(z)))
})

test_that("cbi_compute is NA if any component missing", {
  expect_true(is.na(cbi_compute(0, 0, 0, NA)))
  expect_equal(cbi_compute(1, 1, 1, 1), 1.0)
})

test_that("bivar_tercile_class returns factor with expected levels", {
  x <- 1:9
  y <- 9:1
  cls <- bivar_tercile_class(x, y)
  expect_true(is.factor(cls))
  expect_equal(length(levels(cls)), 9)
  expect_true(any(!is.na(cls)))
})
