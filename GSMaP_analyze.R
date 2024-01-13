rm(list = ls())

library(tidyverse)
library(terra)
library(lubridate)
library(parallel)

gsmap_zip_dir = "../GSMaP_v7_hourly_MVK_zip/"
gsmap_dat_dir = "../GSMaP_v7_hourly_MVK_dat/"

gsmap_zip_files = 
  tibble(Path = list.files(gsmap_zip_dir, recursive = TRUE, full.names = TRUE)) |> 
  tidyr::extract("Path", c("Type", "DateTime", "Version"),
                 "gsmap_(\\S+)\\.(\\d{8}\\.\\d{4})\\.(v\\d)", remove = FALSE) |>
  mutate(DateTime = as.POSIXct(DateTime, format = "%Y%m%d.%H%M"),
         Year = year(DateTime)) |>
  print()
