import pandas as pd
from sqlalchemy import create_engine, text
import random
from datetime import datetime, timedelta

# Lokalni SQL Express, Windows autentizace
SERVER = 'localhost\\SQLEXPRESS'
DATABASE = 'StreamWiseDB'
DRIVER = 'ODBC Driver 17 for SQL Server'

conn_str = f'mssql+pyodbc://@{SERVER}/{DATABASE}?driver={DRIVER}&trusted_connection=yes'
engine = create_engine(conn_str)

print("Zacinam generovat data...")

def generate_data():
    with engine.begin() as conn:
        
        # 1. Vycisteni tabulek
        # Mazani v opacnem poradi nez vkladani kvuli FK
        print("Mazu stare zaznamy z tabulek...")
        conn.execute(text("DELETE FROM fact_AdImpressions"))
        conn.execute(text("DELETE FROM fact_Engagement"))
        conn.execute(text("DELETE FROM dim_UserBase"))
        conn.execute(text("DELETE FROM dim_MediaContent"))
        conn.execute(text("DELETE FROM dim_Devices"))

        # 2. Dimenze zarizeni
                print("Vytvarim seznam zarizeni...")
        devices = [
            {'Platform': 'Android TV', 'AppVersion': '2.4.1', 'ConnectionType': 'WiFi'},
            {'Platform': 'iOS Mobile', 'AppVersion': '3.0.1', 'ConnectionType': '5G'},
            {'Platform': 'Web Browser', 'AppVersion': 'Chrome 120', 'ConnectionType': 'Ethernet'},
            {'Platform': 'Samsung Tizen', 'AppVersion': '1.5.0', 'ConnectionType': 'WiFi'}
        ]
        pd.DataFrame(devices).to_sql('dim_Devices', conn, if_exists='append', index=False)

        # 3. Dimenze obsahu
                print("Vytvarim katalog poradu...")
        content = [
            {'Title': 'Ordinace v Ruzove zahrade 2', 'Category': 'Series', 'SubGenre': 'Soap', 'ProductionCost_USD': 50000, 'Rating_IMDB': 4.5, 'TargetAudience': 'Adults'},
            {'Title': 'Specialiste', 'Category': 'Series', 'SubGenre': 'Crime', 'ProductionCost_USD': 120000, 'Rating_IMDB': 7.2, 'TargetAudience': 'Adults'},
            {'Title': 'Love Island', 'Category': 'Reality', 'SubGenre': 'Romance', 'ProductionCost_USD': 80000, 'Rating_IMDB': 6.0, 'TargetAudience': 'Young Adults'},
            {'Title': 'Televizni Noviny', 'Category': 'News', 'SubGenre': 'Daily', 'ProductionCost_USD': 10000, 'Rating_IMDB': 5.5, 'TargetAudience': 'General'},
            {'Title': 'Harry Potter', 'Category': 'Movie', 'SubGenre': 'Fantasy', 'ProductionCost_USD': 1000000, 'Rating_IMDB': 8.5, 'TargetAudience': 'Kids'},
            {'Title': 'MasterChef Cesko', 'Category': 'Reality', 'SubGenre': 'Cooking', 'ProductionCost_USD': 90000, 'Rating_IMDB': 8.0, 'TargetAudience': 'General'}
        ]
        pd.DataFrame(content).to_sql('dim_MediaContent', conn, if_exists='append', index=False)

        # 4. Dimenze uzivatelu
                print("Generuji uzivatele...")
        users = []
        for _ in range(100):
            users.append({
                'AgeGroup': random.choice(['18-24', '25-34', '35-44', '45-54', '55+']),
                'Gender': random.choice(['M', 'F']),
                'Tier': random.choice(['Free', 'Voyo', 'Premium']),
                'Country': 'CZ',
                'AcquisitionSource': random.choice(['Social', 'Organic', 'TV Ad'])
            })
        pd.DataFrame(users).to_sql('dim_UserBase', conn, if_exists='append', index=False)

        # 5. Faktova tabulka sledovanosti
                print("Simuluji historii sledovanosti (Big Data)...")
        engagements = []
        
        num_users = 100
        num_content = len(content)
        num_devices = len(devices)

        for _ in range(2000):
            content_id = random.randint(1, num_content)
            
                        watch_time = random.randint(2, 120)
            
            # ~30 % prehrani je prerusenych
            is_interrupted = 1 if random.random() < 0.3 else 0 
            
            # Buffering: vetsinou 0, obcas vic
            buffering = random.choices([0, 1, 2, 5], weights=[80, 15, 4, 1])[0]
            
            engagements.append({
                'UserID': random.randint(1, num_users),
                'ContentID': content_id,
                'DeviceID': random.randint(1, num_devices),
                # Poslednich 30 dni
                'StreamStartTimestamp': datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23)),
                'WatchTimeMinutes': watch_time,
                'IsInterrupted': is_interrupted,
                'BufferingEvents': buffering
            })
        
        df_eng = pd.DataFrame(engagements)
        df_eng.to_sql('fact_Engagement', conn, if_exists='append', index=False)
        
        print("Hotovo. Databaze je naplnena daty.")

if __name__ == "__main__":
    generate_data()