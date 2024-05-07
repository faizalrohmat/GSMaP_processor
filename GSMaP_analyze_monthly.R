rm(list = ls())

library(tidyverse)
library(terra)
library(parallel)
library(tictoc)
library(tools)

gsmap_zip_dir_monthly = "D:/GSMaP_test/GSMaP_v7_monthly/"
gsmap_crop_dir = "results/GSMaP_v7_hourly_MVK_crop/"
boundary_path = "D:/GSMaP_test/shp/HSG.shp"

reference_raster_path = "./data/reference_raster.tif"
crop_raster_path = "./data/indonesia_crop_raster.tif"

gsmap_zip_monthly = 
  tibble(Path = list.files(gsmap_zip_dir_monthly, recursive = TRUE, full.names = TRUE)) |> 
  tidyr::extract("Path", c("Type", "YearMonth", "Version"),
                 "gsmap_(\\S+)\\.(\\d{6})\\.\\S+monthly\\.(v\\d)", remove = FALSE) |>
  print()

lon = seq(0.05, 359.95, length.out = 3600)
lat = seq(59.95, -59.95, length.out = 1200)
raster_grid = tibble(expand.grid(x = lon, y = lat)) |>
  mutate(x = if_else(x > 180, x - 360, x))


tic()

cl = makeCluster(pmax(1, floor(detectCores() * 1/4)))
clusterExport(cl, c("gsmap_zip_monthly", "raster_grid", "reference_raster_path",
                    "crop_raster_path", "gsmap_crop_dir", "boundary_path"))
clusterEvalQ(cl, {
  library(tidyverse)
  library(terra)
  library(tools)
})

monthly_precip = 
  parLapply(cl, 1:dim(gsmap_zip_monthly)[[1]], function(i){
    
    reference_raster = terra::rast(reference_raster_path)
    crop_raster = terra::rast(crop_raster_path)
    
    zippath = gsmap_zip_monthly$Path[i]
    
    con = gzcon(file(zippath, "rb"))
    zipvalues = readBin(con, double(), n = 1200 * 3600, size = 4, endian = "little")
    zipvalues[zipvalues < 0] = NA
    close(con)
    
    raster_content = cbind(raster_grid, zipvalues)
    this_raster = terra::rast(raster_content, crs = crs(reference_raster))
    cropped_raster = terra::crop(this_raster, crop_raster)
    
    avg_rain = terra::zonal(cropped_raster, terra::vect(boundary_path))
    ret = tibble(YearMonth =
                   as.Date(paste0(gsmap_zip_monthly$YearMonth[[i]], "01"), "%Y%m%d"), Precip = avg_rain[[1]])
  }) |> 
  bind_rows() |>
  arrange(YearMonth) |>
  print()

toc()

stopCluster(cl)
rm(cl)

ggplot(monthly_precip, aes(YearMonth, Precip)) +
  geom_col() +
  scale_x_date(breaks = "3 month") + 
  theme(axis.text.x = element_text(angle = 90, vjust = 0.5, hjust=1))
  
