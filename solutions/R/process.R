unzip(
  "~/projects/xlsx-challenge/data.zip",
  exdir = "~/projects/xlsx-challenge/solutions/R/xlsx",
)
files <- list.files("xlsx/data", pattern = ".xlsx", full.names = TRUE)

read_all_sheets <- function(file) {
  purrr::map_dfr(
    readxl::excel_sheets(file),
    \(sheet) readxl::read_xlsx(file, sheet = sheet, col_types = "text", col_names = FALSE)
  )
}

df <- files |> purrr::map_dfr(read_all_sheets)
df |> nanoparquet::write_parquet("~/projects/xlsx-challenge/solutions/R/final.parquet")
fs::dir_delete("~/projects/xlsx-challenge/solutions/R/xlsx")

df <- df |>
  dplyr::mutate(
    headers = stringr::str_c(vars(everything())),
    header_type = dplyr::case_when(
      stringr::str_detect(headers, "^row_id.*position$") ~ 1,
      stringr::str_detect(headers, "^row_id.*emp_type$") ~ 2,
      stringr::str_detect(headers, "^first_name*hourly_rate$") ~ 3,
    )
  ) |>
  dplyr::group_by(filepath, sheet) |>
  dplyr::arrange(row) |>
  dplyr::mutate(
    row_group_1 = sum(header_type == 1),
    row_group_2 = sum(header_type == 2),
    row_group_3 = sum(header_type == 3)
  )

final <- dplyr::bind_rows(
  list(
    df |> dplyr::filter(row_group_1),
    df |> dplyr::filter(row_group_2),
    df |> dplyr::filter(row_group_3)
  )
)

xlsx_date_origin <- "1899-12-30"
final |>
  dplyr::mutate(
    dob = as.Date(dob, origin = xlsx_date_origin),
    emp_start = as.Date(emp_start, origin = xlsx_date_origin),
    date = as.Date(date, origin = xlsx_date_origin)
  ) |>
  dplyr::group_by(first_name, last_name, dob, date) |>
  dplyr::arrange(filepath) |>
  dplyr::filter(dplyr::row_number() == 1) |>
  print()
