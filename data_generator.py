import pandas as pd
from sqlalchemy import create_engine, text
import random
from datetime import datetime, timedelta

# Tady nastavuji pripojeni k moji lokalni databazi
# SERVER: localhost\SQLEXPRESS je moje lokalni instance SQL Serveru
# DATABASE: StreamWiseDB je databaze, kterou jsem si pripravila v SSMS
SERVER = 'localhost\\SQLEXPRESS'
DATABASE = 'StreamWiseDB'
DRIVER = 'ODBC Driver 17 for SQL Server'

# Vytvorim pripojovaci retezec (connection string), ktery slouzi jako most mezi Pythonem a SQL
conn_str = f'mssql+pyodbc://@{SERVER}/{DATABASE}?driver={DRIVER}&trusted_connection=yes'
engine = create_engine(conn_str)

print("Zacinam generovat data...")

def generate_data():
    # Otevru spojeni s databazi
    with engine.begin() as conn:
        
        # 1. KROK: CISTENI
        # Nejdriv smazu stara data, abych pri kazdem spusteni mela cisty stul a nezdvojovala se mi data.
        # Mazani musi byt v opacnem poradi nez vkladani kvuli vazbam (Foreign Keys).
        print("Mazu stare zaznamy z tabulek...")
        conn.execute(text("DELETE FROM fact_AdImpressions"))
        conn.execute(text("DELETE FROM fact_Engagement"))
        conn.execute(text("DELETE FROM dim_UserBase"))
        conn.execute(text("DELETE FROM dim_MediaContent"))
        conn.execute(text("DELETE FROM dim_Devices"))

        # 2. KROK: DIMENZE ZARIZENI
        # Simuluji ruzne platformy, na kterych lide sleduji obsah (TV, mobil, web).
        print("Vytvarim seznam zarizeni...")
        devices = [
            {'Platform': 'Android TV', 'AppVersion': '2.4.1', 'ConnectionType': 'WiFi'},
            {'Platform': 'iOS Mobile', 'AppVersion': '3.0.1', 'ConnectionType': '5G'},
            {'Platform': 'Web Browser', 'AppVersion': 'Chrome 120', 'ConnectionType': 'Ethernet'},
            {'Platform': 'Samsung Tizen', 'AppVersion': '1.5.0', 'ConnectionType': 'WiFi'}
        ]
        # Dataframe poslu rovnou do SQL tabulky dim_Devices
        pd.DataFrame(devices).to_sql('dim_Devices', conn, if_exists='append', index=False)

        # 3. KROK: DIMENZE OBSAHU
        # Tady vytvarim katalog filmu a serialu. Snazim se o mix zanru, aby data davala smysl.
        # Pridavam i naklady a hodnoceni, abychom meli co analyzovat.
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

        # 4. KROK: DIMENZE UZIVATELU
        # Generuji 100 nahodnych uzivatelu.
        # Pro kazdeho nahodne vyberu vek, pohlavi a typ predplatneho.
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

        # 5. KROK: FAKTOVA TABULKA (HISTORIE SLEDOVANI)
        # Toto je nejdulezitejsi cast. Simuluji 2000 prehrani ruznych poradu ruznymi lidmi.
        print("Simuluji historii sledovanosti (Big Data)...")
        engagements = []
        
        # Potrebuji znat pocty zaznamu, abych mohl generovat spravna ID
        num_users = 100
        num_content = len(content)
        num_devices = len(devices)

        for _ in range(2000):
            # Nahodne vyberu, kdo se diva a na co
            content_id = random.randint(1, num_content)
            
            # Logika pro sledovani:
            # Generuji nahodny cas sledovani mezi 2 a 120 minutami
            watch_time = random.randint(2, 120)
            
            # Nahodne urcim, jestli to uzivatel vypnul predcasne (cca 30% sance)
            is_interrupted = 1 if random.random() < 0.3 else 0 
            
            # Simulace technickych problemu (buffering) - vetsinou 0, obcas se to sekne
            buffering = random.choices([0, 1, 2, 5], weights=[80, 15, 4, 1])[0]
            
            # Pridani zaznamu do seznamu
            engagements.append({
                'UserID': random.randint(1, num_users),
                'ContentID': content_id,
                'DeviceID': random.randint(1, num_devices),
                # Nahodny cas v poslednich 30 dnech
                'StreamStartTimestamp': datetime.now() - timedelta(days=random.randint(0, 30), hours=random.randint(0, 23)),
                'WatchTimeMinutes': watch_time,
                'IsInterrupted': is_interrupted,
                'BufferingEvents': buffering
            })
        
        # Ulozim vsechna data o sledovanosti do databaze najednou
        df_eng = pd.DataFrame(engagements)
        df_eng.to_sql('fact_Engagement', conn, if_exists='append', index=False)
        
        print("Hotovo. Databaze je naplnena daty.")

if __name__ == "__main__":
    generate_data()