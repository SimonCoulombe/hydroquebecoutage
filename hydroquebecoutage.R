library(tidyverse)
library(rvest)
library(purrr)
data_dir <-  "data/"

url <- "http://pannes.hydroquebec.com/pannes/donnees/v3_0/open_data/"

page <- read_html(url)
file_names_available <- page %>% html_elements("a") %>% html_attr("href")
file_names_to_download <- file_names_available[!file_names_available %in% list.files(data_dir)]
urls <- paste0(url, file_names_to_download)

## use possible so the script doesnt  fail on 404 error https://www.r-bloggers.com/2020/08/handling-errors-using-purrrs-possibly-and-safely/
possibly.download.file <- possibly(.f = download.file, otherwise = "Error")
purrr::map2(urls, file_names_to_download, ~ possibly.download.file(url =.x, destfile = paste0(data_dir, .y)))

