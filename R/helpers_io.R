assert_project_root <- function() {
  # `data-raw/` is used for ETL scripts + caches, but it is not required for
  # running the deployed Shiny app (and is typically excluded from deployment
  # bundles via `.rscignore`).
  needed <- c("R", "data", "www")
  missing <- needed[!dir.exists(needed)]
  if (length(missing) > 0) {
    stop(
      "Run this from the project root (missing dirs: ",
      paste(missing, collapse = ", "),
      "). Current working directory: ",
      getwd(),
      call. = FALSE
    )
  }
  invisible(TRUE)
}

proj_path <- function(...) {
  assert_project_root()
  file.path(getwd(), ...)
}

ensure_dir <- function(path) {
  if (!dir.exists(path)) dir.create(path, recursive = TRUE, showWarnings = FALSE)
  invisible(path)
}

require_pkgs <- function(pkgs) {
  missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing) > 0) {
    stop(
      "Missing required R packages: ",
      paste(missing, collapse = ", "),
      "\nInstall them (recommended): renv::restore()",
      call. = FALSE
    )
  }
  invisible(TRUE)
}

read_parquet <- function(path) {
  require_pkgs(c("arrow"))
  arrow::read_parquet(path)
}

write_parquet <- function(x, path) {
  require_pkgs(c("arrow"))
  ensure_dir(dirname(path))
  arrow::write_parquet(x, path)
  invisible(path)
}
