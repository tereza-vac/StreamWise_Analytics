import pandas as pd
from sqlalchemy import create_engine, text
import os

# --- 1. NASTAVENÍ (Srdce celého skriptu) ---
# Tady říkáme Pythonu, kde najde naši databázi.
# Je to jako když zadáváš adresu do navigace.
SERVER = 'localhost\\SQLEXPRESS'
DATABASE = 'StreamWiseDB'
DRIVER = 'ODBC Driver 17 for SQL Server'

# Spojovací řetězec (Connection String) - klíč, který odemkne databázi
conn_str = f'mssql+pyodbc://@{SERVER}/{DATABASE}?driver={DRIVER}&trusted_connection=yes'
engine = create_engine(conn_str)

print("Začínám nasazovat analytiku do databáze...")

def deploy_and_export():
    # Otevřeme "tunel" do databáze
    with engine.begin() as conn:
        
        # --- 2. VIEW: VÝKONNOST OBSAHU (Co lidi baví?) ---
        # Business otázka: Které seriály jsou trháky a u kterých lidi usínají?
        # Proč to děláme: Aby dramaturgové věděli, co točit dál.
        print("Vytvářím pohled 'v_ContentPerformance'...")
        conn.execute(text("""
        CREATE OR ALTER VIEW v_ContentPerformance AS
        SELECT 
            c.Title,               -- Název
            c.Category,            -- Žánr
            COUNT(e.EngagementID) AS TotalViews, -- Kolikrát si to někdo pustil
            
            -- Složitější výpočet: Drop-off rate (Míra odpadlíků)
            -- Pokud to hodně lidí vypne před koncem, je to špatné znamení.
            FORMAT(SUM(CAST(e.IsInterrupted AS INT)) * 1.0 / COUNT(e.EngagementID), 'P') AS DropOffRate,
            
            -- Žebříček: Seřadíme filmy podle sledovanosti v rámci kategorie
            DENSE_RANK() OVER (PARTITION BY c.Category ORDER BY COUNT(e.EngagementID) DESC) as CategoryRank
        FROM fact_Engagement e
        JOIN dim_MediaContent c ON e.ContentID = c.ContentID
        GROUP BY c.Title, c.Category;
        """))

        # --- 3. VIEW: PŘÍPRAVA PRO AI (Kdo chce odejít?) ---
        # Business otázka: Jak se chovají uživatelé? Seká se jim to?
        # Proč to děláme: Data Science tým potřebuje čistá data pro predikci odchodů (Churn).
        print("Vytvářím pohled 'v_UserChurnFeatures'...")
        conn.execute(text("""
        CREATE OR ALTER VIEW v_UserChurnFeatures AS
        SELECT 
            u.UserID,
            u.Tier,                -- Typ předplatného
            COUNT(e.EngagementID) as TotalSessions,      -- Kolikrát přišel
            SUM(e.WatchTimeMinutes) as TotalMinutesWatched, -- Kolik času u nás nechal
            SUM(e.BufferingEvents) as TotalBufferingEvents  -- Kolikrát se mu to seklo (Důležité!)
        FROM dim_UserBase u
        LEFT JOIN fact_Engagement e ON u.UserID = e.UserID
        GROUP BY u.UserID, u.Tier;
        """))

        # --- 4. VIEW: PRIME TIME & PENÍZE (Kdy vyděláváme?) ---
        # Business otázka: Která hodina je pro nás zlatý důl?
        # Proč to děláme: Aby marketing věděl, kdy nasazovat reklamy.
        print("Vytvářím pohled 'v_PrimeTimeAnalytics'...")
        conn.execute(text("""
        CREATE OR ALTER VIEW v_PrimeTimeAnalytics AS
        SELECT 
            DATEPART(HOUR, e.StreamStartTimestamp) as HourOfDay, -- Hodina dne (0-23)
            COUNT(DISTINCT e.UserID) as ActiveUsers,             -- Kolik unikátních lidí koukalo
            
            -- Odhad tržby (Revenue):
            -- Free uživatel = 5 centů/minuta (reklama), Premium = 1 cent/minuta (paušál)
            SUM(CASE 
                WHEN u.Tier = 'Free' THEN e.WatchTimeMinutes * 0.05 
                ELSE e.WatchTimeMinutes * 0.01 
            END) as EstimatedRevenue_USD
        FROM fact_Engagement e
        JOIN dim_UserBase u ON e.UserID = u.UserID
        GROUP BY DATEPART(HOUR, e.StreamStartTimestamp);
        """))

        # --- 5. VIEW: DENNÍ RŮST (Novinka!) ---
        # Business otázka: Roste nám sledovanost den po dni?
        print("Vytvářím pohled 'v_DailyTraffic'...")
        conn.execute(text("""
        CREATE OR ALTER VIEW v_DailyTraffic AS
        SELECT 
            CAST(e.StreamStartTimestamp AS DATE) as Date, -- Ořízneme čas, necháme jen datum
            COUNT(e.EngagementID) as TotalStreams,        -- Počet přehrání ten den
            COUNT(DISTINCT e.UserID) as UniqueViewers     -- Kolik různých lidí přišlo
        FROM fact_Engagement e
        GROUP BY CAST(e.StreamStartTimestamp AS DATE);
        """))

    # --- 6. EXPORT PRO POWER BI ---
    # Tady bereme data z SQL a ukládáme je do CSV souborů, 
    # protože Power BI si s CSV skvěle rozumí.
    print("\n Exportuji data do složky 'bi_exports'...")
    
    # Pokud složka neexistuje, vyrobíme ji
    if not os.path.exists('bi_exports'):
        os.makedirs('bi_exports')

    # Načteme data z Views do paměti (DataFrames)
    df_content = pd.read_sql("SELECT * FROM v_ContentPerformance", engine)
    df_ai = pd.read_sql("SELECT * FROM v_UserChurnFeatures", engine)
    df_prime = pd.read_sql("SELECT * FROM v_PrimeTimeAnalytics ORDER BY HourOfDay", engine)
    df_daily = pd.read_sql("SELECT * FROM v_DailyTraffic ORDER BY Date", engine)

    # Uložíme na disk
    df_content.to_csv('bi_exports/content_report.csv', index=False)
    df_ai.to_csv('bi_exports/ai_features.csv', index=False)
    df_prime.to_csv('bi_exports/prime_time_revenue.csv', index=False)
    df_daily.to_csv('bi_exports/daily_growth.csv', index=False)

    print("HOTOVO! Všechna data jsou připravena pro Power BI.")
    print(f"Vygenerováno {len(df_daily)} dní historie.")

if __name__ == "__main__":
    deploy_and_export()