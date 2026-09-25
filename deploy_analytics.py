import pandas as pd
from sqlalchemy import create_engine, text
import os

# --- 1. Připojení k DB ---
SERVER = 'localhost\\SQLEXPRESS'
DATABASE = 'StreamWiseDB'
DRIVER = 'ODBC Driver 17 for SQL Server'

# Windows autentizace, lokální SQL Express
conn_str = f'mssql+pyodbc://@{SERVER}/{DATABASE}?driver={DRIVER}&trusted_connection=yes'
engine = create_engine(conn_str)

print("Začínám nasazovat analytiku do databáze...")

def deploy_and_export():
    with engine.begin() as conn:
        
        # --- 2. View: výkonnost obsahu ---
        # Sledovanost, drop-off rate a pořadí v rámci kategorie
        print("Vytvářím pohled 'v_ContentPerformance'...")
        conn.execute(text("""
        CREATE OR ALTER VIEW v_ContentPerformance AS
        SELECT 
            c.Title,
            c.Category,
            COUNT(e.EngagementID) AS TotalViews,
            
            -- Drop-off rate: podíl přerušených přehrání
            FORMAT(SUM(CAST(e.IsInterrupted AS INT)) * 1.0 / COUNT(e.EngagementID), 'P') AS DropOffRate,
            
            -- Pořadí podle sledovanosti v kategorii
            DENSE_RANK() OVER (PARTITION BY c.Category ORDER BY COUNT(e.EngagementID) DESC) as CategoryRank
        FROM fact_Engagement e
        JOIN dim_MediaContent c ON e.ContentID = c.ContentID
        GROUP BY c.Title, c.Category;
        """))

        # --- 3. View: features pro churn model ---
        # Jeden řádek = jeden uživatel
        print("Vytvářím pohled 'v_UserChurnFeatures'...")
        conn.execute(text("""
        CREATE OR ALTER VIEW v_UserChurnFeatures AS
        SELECT 
            u.UserID,
            u.Tier,
            COUNT(e.EngagementID) as TotalSessions,
            SUM(e.WatchTimeMinutes) as TotalMinutesWatched,
            SUM(e.BufferingEvents) as TotalBufferingEvents
        FROM dim_UserBase u
        LEFT JOIN fact_Engagement e ON u.UserID = e.UserID
        GROUP BY u.UserID, u.Tier;
        """))

        # --- 4. View: prime time a odhad tržeb ---
        # Aktivní uživatelé a tržby podle hodiny dne
        print("Vytvářím pohled 'v_PrimeTimeAnalytics'...")
        conn.execute(text("""
        CREATE OR ALTER VIEW v_PrimeTimeAnalytics AS
        SELECT 
            DATEPART(HOUR, e.StreamStartTimestamp) as HourOfDay, -- 0-23
            COUNT(DISTINCT e.UserID) as ActiveUsers,
            
            -- Odhad tržby:
            -- Free uživatel = 5 centů/minuta (reklama), Premium = 1 cent/minuta (paušál)
            SUM(CASE 
                WHEN u.Tier = 'Free' THEN e.WatchTimeMinutes * 0.05 
                ELSE e.WatchTimeMinutes * 0.01 
            END) as EstimatedRevenue_USD
        FROM fact_Engagement e
        JOIN dim_UserBase u ON e.UserID = u.UserID
        GROUP BY DATEPART(HOUR, e.StreamStartTimestamp);
        """))

        # --- 5. View: denní provoz ---
        # Počet přehrání a unikátních diváků po dnech
        print("Vytvářím pohled 'v_DailyTraffic'...")
        conn.execute(text("""
        CREATE OR ALTER VIEW v_DailyTraffic AS
        SELECT 
            CAST(e.StreamStartTimestamp AS DATE) as Date,
            COUNT(e.EngagementID) as TotalStreams,
            COUNT(DISTINCT e.UserID) as UniqueViewers
        FROM fact_Engagement e
        GROUP BY CAST(e.StreamStartTimestamp AS DATE);
        """))

    # --- 6. Export CSV pro Power BI ---
    # Views -> bi_exports/*.csv
    print("\n Exportuji data do složky 'bi_exports'...")
    
    if not os.path.exists('bi_exports'):
        os.makedirs('bi_exports')

    df_content = pd.read_sql("SELECT * FROM v_ContentPerformance", engine)
    df_ai = pd.read_sql("SELECT * FROM v_UserChurnFeatures", engine)
    df_prime = pd.read_sql("SELECT * FROM v_PrimeTimeAnalytics ORDER BY HourOfDay", engine)
    df_daily = pd.read_sql("SELECT * FROM v_DailyTraffic ORDER BY Date", engine)

    df_content.to_csv('bi_exports/content_report.csv', index=False)
    df_ai.to_csv('bi_exports/ai_features.csv', index=False)
    df_prime.to_csv('bi_exports/prime_time_revenue.csv', index=False)
    df_daily.to_csv('bi_exports/daily_growth.csv', index=False)

    print("HOTOVO! Všechna data jsou připravena pro Power BI.")
    print(f"Vygenerováno {len(df_daily)} dní historie.")

if __name__ == "__main__":
    deploy_and_export()