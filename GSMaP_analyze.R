rm(list = ls())

library(tidyverse)
library(terra)
library(parallel)
library(tictoc)

gsmap_zip_dir = "data/GSMaP_v7_hourly_MVK_zip/"
gsmap_crop_dir = "results/GSMaP_v7_hourly_MVK_crop/"

reference_raster_path = "./data/reference_raster.tif"
crop_raster_path = "./data/indonesia_crop_raster.tif"

gsmap_zip_files = 
  tibble(Path = list.files(gsmap_zip_dir, recursive = TRUE, full.names = TRUE)) |> 
  tidyr::extract("Path", c("Type", "DateTime", "Version"),
                 "gsmap_(\\S+)\\.(\\d{8}\\.\\d{4})\\.(v\\d)", remove = FALSE) |>
  mutate(DateTime = as.POSIXct(DateTime, format = "%Y%m%d.%H%M"),
         Year = year(DateTime)) |>
  print()

lon = seq(0.05, 359.95, length.out = 3600)
lat = seq(59.95, -59.95, length.out = 1200)
raster_grid = tibble(expand.grid(x = lon, y = lat)) |>
  mutate(x = if_else(x > 180, x - 360, x))

tic()

cl = makeCluster(pmax(1, floor(detectCores() * 2/3)))
clusterExport(cl, c("gsmap_zip_files", "raster_grid", "reference_raster_path",
                    "crop_raster_path", "gsmap_crop_dir"))
clusterEvalQ(cl, {
  library(tidyverse)
  library(terra)
})

gsmap_unzip_files = 
  parLapply(cl, 1:dim(gsmap_zip_files)[[1]], function(i){
    
    reference_raster = terra::rast(reference_raster_path)
    crop_raster = terra::rast(crop_raster_path)
    
    zippath = gsmap_zip_files$Path[i]
    
    con = gzcon(file(zippath, "rb"))
    zipvalues = readBin(con, double(), n = 1200 * 3600, size = 4, endian = "little")
    zipvalues[zipvalues < 0] = NA
    close(con)
    
    raster_content = cbind(raster_grid, zipvalues)
    this_raster = terra::rast(raster_content, crs = crs(reference_raster))
    cropped_raster = terra::crop(this_raster, crop_raster)
    
    savepath = paste0(file.path(gsmap_crop_dir, 
                                gsmap_zip_files$Year,
                                basename(tools::file_path_sans_ext(zippath))),
                      ".tif")
    
    dir.create(dirname(savepath))
    
    if (!file.exists(savepath)) {
      writeRaster(cropped_raster, savepath)
    }
    
})

stopCluster(cl)

toc()
