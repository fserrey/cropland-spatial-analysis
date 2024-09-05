# Cropland Data Analysis with PostGIS

This project sets up a Dockerized PostgreSQL database with PostGIS extension to store and analyze cropland data from the USDA Cropland Data Layer (CDL) and the Natural Earth "Admin 1 – States, Provinces" shapefile. The project includes scripts to load vector and raster data into the database and perform geospatial analysis to compute cropland fractions and total hectarage for specific crops (corn, soy, spring wheat, and winter wheat) for each state in the USA from 2020 to 2023.

## Prerequisites

- [Docker](https://www.docker.com/)
- [Python 3.x](https://www.python.org/downloads/)
- Required Python libraries:
  - `geopandas`
  - `psycopg2`
  - `sqlalchemy`
  - `pandas`
  - `dotenv`

## Setup Instructions

### 1. Build the Docker Container

Create a Docker container with PostgreSQL and PostGIS using the provided `Dockerfile`:

1. Clone this repository and navigate to its directory:

   ```sh
   git clone <your-repo-url>
   cd <your-repo-directory>
   ```
2. Build the Docker image:
    ```sh
       docker build -t postgis_container .
    ```
3. Build the Docker image:
    ```sh
       docker run -d -p 5432:5432 --name postgis_container postgis_container
    ```
   

