import requests, time
from sqlalchemy import create_engine, text
from utils import Dev, get_logger

params = Dev()
server = params.server
database = params.database

logger = get_logger()

con_str = f"mssql+pyodbc://{server}/{database}\
            ?driver=ODBC+Driver+17+for+SQL+Server"

engine = create_engine(con_str, echo=False, pool_pre_ping=True)

def extract_data():
    try:
        logger.info('[Extracting data]...')
        response = requests.get("https://opensky-network.org/api/states/all")
        if response.status_code != 200:
            logger.error(f"Failed to fetch data : {response.status_code}")
        logger.info(f"response : {response.status_code}")
        return response.json()
    except Exception as e:
        logger.exception(f"Error in fetching data : {e}")
        return None

def load_data(data):
    try:
        logger.info("[Loading data]...")
        with engine.begin() as conn:
            for flight in data['states']:
                try:
                    conn.execute(
                        text(
                            "EXEC dbo.upsert_flights :flight_id, :call_sign, :origin_country, :time_position, :last_contact, :longitude, :latitude, :altitude, :on_ground, :velocity;"
                        ),
                        {
                            'flight_id' : flight[0],
                            'call_sign' : flight[1],
                            'origin_country' : flight[2],
                            'time_position' : flight[3],
                            'last_contact' : flight[4],
                            'longitude' : flight[5],
                            'latitude' : flight[6],
                            'altitude' : flight[7],
                            'on_ground' : flight[8],
                            'velocity' : flight[9]
                        }
                    )
                except Exception as e:
                    logger.exception(f"Upsert trigger failed for entry {flight} : {e}")
        return True
    except Exception as e:
        logger.exception(f"Upsert trigger failed : {e}")
        return False

def transform_data():
    try:
        logger.info("[Transforming data]...")
        with engine.begin() as conn:
            conn.execute(
                text(
                    "EXEC dbo.clean_flights;"
                )
            )
        logger.info("Flight data transformation success!")
    except Exception as e:
        logger.exception(f"Transformation failed : {e}")

def retry(func, max_retries = 3, delay = 5):
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt to exract data : {attempt}")
            return func()
        except Exception as e:
            logger.exception(f"Retry attempt {attempt} failed : {e}")
            time.sleep(delay)

if __name__ == "__main__":
    try:
        data = retry(extract_data)
        if load_data(data):
            transform_data()
        print("ELT DONE!")
    except Exception as e:
        logger.critical(f"Pipeline failed due to : {e}")
