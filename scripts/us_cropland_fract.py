import os
from dotenv import load_dotenv
from sqlite3 import OperationalError
import psycopg2
import pandas as pd
from sqlalchemy import create_engine

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
env_path = os.path.join(project_root, 'var.env')

load_dotenv(dotenv_path=env_path)

DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')

# Create engine
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

try:
    with engine.connect() as conn:
        print("Connection to the PostgreSQL database was successful.")
except Exception as e:
    print(f"An error occurred while connecting to the PostgreSQL database: {e}")
    exit()

# CDL crop codes
CROP_CODES = {
    'corn': 1,
    'soy': 5,
    'spring_wheat': 23,
    'winter_wheat': 24
}


def calculate_cropland_fraction_and_hectarage(engine, year):
    """Calculate the cropland fraction and hectarage for the specified year."""

    results = []

    with engine.connect() as conn:
        try:
            query = f"""
                WITH state_area AS (
                    SELECT
                        s.name AS state,
                        COALESCE(ST_Area(ST_Transform(s.geometry, 5070)) / 10000, 0) AS state_area_ha,
                        s.geometry AS state_geom
                    FROM us_states s
                    WHERE s.iso_a2 = 'US'
                ),
                crop_area AS (
                    SELECT
                        sa.state,
                        CASE 
                            WHEN ST_Value(r.rast, 1, 1) = 1 THEN 'corn'
                            WHEN ST_Value(r.rast, 1, 1) = 5 THEN 'soy'
                            WHEN ST_Value(r.rast, 1, 1) = 23 THEN 'spring_wheat'
                            WHEN ST_Value(r.rast, 1, 1) = 24 THEN 'winter_wheat'
                        END AS crop,
                        COALESCE(ST_Area(
                            ST_Intersection(ST_Transform(sa.state_geom, 5070) , r.rast::geometry)
                        ) / 10000.0, 0) AS crop_area_ha
                    FROM cdl_{year} r
                    JOIN state_area sa ON ST_Intersects(sa.state_geom, ST_Transform(r.rast::geometry, 4326))
                    WHERE ST_Value(r.rast, 1, 1) IN ({','.join(map(str, CROP_CODES.values()))})
                ),
                crop_summary AS (
                    SELECT
                        state,
                        crop,
                        COALESCE(SUM(crop_area_ha), 0) AS total_crop_area_ha
                    FROM crop_area
                    GROUP BY state, crop
                )
                SELECT
                    cs.state,
                    cs.crop,
                    ROUND(cast(cs.total_crop_area_ha as numeric), 2) as total_crop_area_ha,
                    ROUND(cast(sa.state_area_ha as numeric), 2) as state_area_ha,
                    ROUND(CAST((cs.total_crop_area_ha / sa.state_area_ha) * 100 AS numeric), 2) AS crop_fraction
                FROM crop_summary cs
                JOIN state_area sa ON cs.state = sa.state
                order by 1;

            """

            query_result = conn.execute(query).fetchall()
            for row in query_result:
                results.append({
                    'year': year,
                    'state': row[0],
                    'crop': row[1],
                    'total_crop_area_ha': row[2],
                    'crop_fraction': row[4]  # jump state ha area
                })

        except OperationalError as e:
            print(f"Error querying state for year {year}: {e}")
            conn.rollback()

        return pd.DataFrame(results)


def main():
    """Main function to execute the script and loop through the years and perform calculations"""
    output_folder = os.path.join(project_root, 'output')

    # Check if the output folder exists, if not, create it
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
        print(f"Created output directory at {output_folder}")

    all_results = pd.DataFrame()
    #
    for year in range(2020, 2024):
        df = calculate_cropland_fraction_and_hectarage(engine, year)
        all_results = pd.concat([all_results, df], ignore_index=True)

    print(all_results.head())

    all_results.to_csv('../output/cropland_fraction_hectarage.csv', index=False)

    # Close the database connection
    engine.dispose()

if __name__ == "__main__":
    main()