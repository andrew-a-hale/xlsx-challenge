path <- file.path("../../data.zip")
files <- unzip(
  path,
  exdir = "xlsx",
  unzip = "unzip"
)

files <- list.files("xlsx/data", pattern = ".xlsx", full.names = TRUE)

read_all_sheets <- function(file) {
  purrr::map_dfr(
    readxl::excel_sheets(file),
    \(sheet) {
      file |>
        readxl::read_xlsx(sheet = sheet, col_types = "text", col_names = FALSE) |>
        dplyr::mutate(filepath = file, sheet = sheet, row = dplyr::row_number())
    }
  )
}

if (!file.exists("final.parquet")) {
  df <- files |> purrr::map_dfr(read_all_sheets)
  df |> nanoparquet::write_parquet("final.parquet")
  fs::dir_delete("xlsx")
}

int_to_letter <- function(col_names) {
  col_names |>
    purrr::map_chr(
      \(col) {
        x <- col |>
          stringr::str_remove_all("\\.") |>
          as.integer()
        if (is.na(x)) {
          col
        } else {
          LETTERS[x]
        }
      }
    )
}

df <- nanoparquet::read_parquet("final.parquet") |>
  tidyr::unite("headers", 1:8, remove = FALSE, sep = ",") |>
  dplyr::mutate(
    header_type = dplyr::case_when(
      stringr::str_detect(headers, "^row_id.*position$") ~ 1,
      stringr::str_detect(headers, "^row_id.*emp_type$") ~ 2,
      stringr::str_detect(headers, "^first_name.*emp_type$") ~ 3
    )
  ) |>
  dplyr::group_by(filepath, sheet) |>
  dplyr::arrange(row) |>
  dplyr::mutate(
    row_group_1 = sum(header_type == 1, na.rm = TRUE),
    row_group_2 = sum(header_type == 2, na.rm = TRUE),
    row_group_3 = sum(header_type == 3, na.rm = TRUE)
  ) |>
  dplyr::ungroup() |>
  dplyr::rename_with(int_to_letter)

EMP_TYPES <- c("CASUAL", "CONTRACT", "PART_TIME", "FULL_TIME", "TEMPORARY")

final <- dplyr::bind_rows(
  list(
    df |>
      dplyr::filter(row_group_1 > 0, `F` %in% EMP_TYPES) |>
      dplyr::mutate(
        first_name = dplyr::case_when(
          stringr::str_length(`B`) == 0 ~ "",
          TRUE ~ stringr::str_split_i(`B`, " ", 2)
        ),
        last_name = dplyr::case_when(
          stringr::str_length(`B`) == 0 ~ "",
          TRUE ~ stringr::str_split_i(`B`, ",", 1)
        )
      ) |>
      dplyr::select(
        filepath,
        sheet,
        row_id = `A`,
        first_name,
        last_name,
        dob = `C`,
        emp_start = `D`,
        date = `E`,
        emp_type = `F`,
        hourly_rate = `G`,
        position = `H`
      ),
    df |>
      dplyr::filter(row_group_2 > 0, `H` %in% EMP_TYPES) |>
      dplyr::mutate(
        first_name = dplyr::case_when(
          length(`B`) == 0 ~ "",
          TRUE ~ stringr::str_split_i(`B`, " ", 1)
        ),
        last_name = dplyr::case_when(
          length(`B`) == 0 ~ "",
          TRUE ~ stringr::str_split_i(`B`, " ", 2)
        )
      ) |>
      dplyr::select(
        filepath,
        sheet,
        row_id = `A`,
        first_name,
        last_name,
        dob = `C`,
        emp_start = `G`,
        date = `D`,
        emp_type = `H`,
        hourly_rate = `F`,
        position = `E`
      ),
    df |>
      dplyr::filter(row_group_3 > 0, `H` %in% EMP_TYPES) |>
      dplyr::select(
        filepath,
        sheet,
        row_id = `D`,
        first_name = `A`,
        last_name = `B`,
        dob = `C`,
        emp_start = `E`,
        date = `F`,
        emp_type = `H`,
        hourly_rate = `I`,
        position = `H`
      )
  )
)

xlsx_date_origin <- "1899-12-30"
final |>
  dplyr::mutate(
    dob = as.Date(as.integer(dob), origin = xlsx_date_origin),
    emp_start = as.Date(as.integer(emp_start), origin = xlsx_date_origin),
    date = as.Date(as.integer(date), origin = xlsx_date_origin)
  ) |>
  dplyr::group_by(first_name, last_name, dob, date) |>
  dplyr::arrange(filepath) |>
  dplyr::filter(dplyr::row_number() == 1) |>
  dplyr::ungroup() |>
  dplyr::arrange(first_name, last_name, dob, date) |>
  print()
