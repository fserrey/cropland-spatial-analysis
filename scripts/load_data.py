import os
import geopandas as gpd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')

SHAPEFILE_PATH = os.getenv('SHAPEFILE_PATH')
CDL_BASE_PATH = os.getenv('CDL_BASE_PATH')
CDL_YEARS = list(map(int, os.getenv('CDL_YEARS').split(',')))


def connect_to_db():
    """Creates a connection to the PostgreSQL/PostGIS database."""
    engine = create_engine(
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    try:
        with engine.connect() as conn:
            print("Connection to the PostgreSQL database was successful.")
    except Exception as e:
        print(f"An error occurred while connecting to the PostgreSQL database: {e}")
        exit()
    return engine


def load_vector_data(engine):
    """Loads vector data from the Natural Earth shapefile into the PostGIS database."""
    try:
        states_gdf = gpd.read_file(SHAPEFILE_PATH)
        states_gdf.to_postgis('us_states', engine, if_exists='replace', index=False)
        print("Vector data loaded successfully.")
    except Exception as e:
        print(f"An error occurred while loading vector data: {e}")


def load_raster_data():
    """Loads raster data from the USDA CDL into the PostGIS database."""
    for year in CDL_YEARS:
        cdl_path = os.path.join(CDL_BASE_PATH, f"{year}_30m_cdls/{year}_30m_cdls.tif")
        gdal_cmd = (
            f"raster2pgsql -s 4326 -I -C -e -M -t 500x500 {cdl_path} public.cdl_{year} "
            f"| PGPASSWORD={POSTGRES_PASSWORD} psql -d {POSTGRES_DB} -U {POSTGRES_USER} -h {POSTGRES_HOST} -p {POSTGRES_PORT}"
        )
        try:
            os.system(gdal_cmd)
            print(f"Raster data for {year} loaded successfully.")
        except Exception as e:
            print(f"An error occurred while loading raster data for {year}: {e}")


def main():
    engine = connect_to_db()
    load_vector_data(engine)
    load_raster_data()


if __name__ == "__main__":
    main()