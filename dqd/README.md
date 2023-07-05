# Data Quality Dashboard (DQD)

This module is responsible for building an environment for and running the [OHDSI Data Quality Dashbaord](https://github.com/OHDSI/DataQualityDashboard) on AoU EHR data.

There are two subdirectories controlling the two stages of the execution piepline.

1. build - Configures and builds an R-compatible RedHat container.
2. run - Runs the DQD R package and saves output to a google bucket.

After the stages have been completed, the DQD output can be viewed in a web browser with the following command:

`Rscript -e "library(DataQualityDashboard); DataQualityDashboard::viewDqDashboard(path/to/results.json')"`