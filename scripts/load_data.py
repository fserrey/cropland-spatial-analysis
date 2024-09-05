import os
import geopandas as gpd
from sqlalchemy import create_engine
from dotenv import load_dotenv

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(project_root, 'var.env')

load_dotenv(dotenv_path=env_path)

DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

SHAPEFILE_PATH=os.path.join(project_root, 'data/admin1/ne_10m_admin_1_states_provinces.shp')
CDL_BASE_PATH=os.path.join(project_root, 'data/CDL')
CDL_YEARS= [2020,2021,2022,2023]


def connect_to_db():
    """Creates a connection to the PostgreSQL/PostGIS database."""
    engine = create_engine(
        f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
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
        states_gdf = states_gdf.to_crs(epsg=4326)
        states_gdf.to_postgis('us_states', engine, if_exists='replace', index=False)
        print("Vector data loaded successfully.")
    except Exception as e:
        print(f"An error occurred while loading vector data: {e}")


def load_raster_data():
    """Loads raster data from the USDA CDL into the PostGIS database."""
    for year in CDL_YEARS:
        cdl_path = os.path.join(CDL_BASE_PATH, f"{year}_30m_cdls/{year}_30m_cdls.tif")
        gdal_cmd = (
            f"raster2pgsql -s 5070 -I -C -e -M -t 500x500 {cdl_path} public.cdl_{year} "
            f"| PGPASSWORD={DB_PASSWORD} psql -d {DB_NAME} -U {DB_USER} -h {DB_HOST} -p {DB_PORT}"
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