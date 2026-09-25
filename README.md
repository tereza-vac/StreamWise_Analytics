# StreamWise Analytics

End-to-end datová pipeline pro fiktivní streamovací platformu: od generování syntetických dat přes hvězdicové schéma v SQL Serveru až po reporting v Power BI a přípravu features pro model predikce odchodu uživatelů (churn).

## Co projekt obsahuje

- **Analýza sledovanosti** – prime time, odhad tržeb, denní provoz.
- **Segmentace uživatelů** – typ předplatného, demografie, oblíbené žánry.
- **Features pro churn model** – vztah mezi věrností uživatele a technickými problémy (buffering).

## Technologie

- **Python** (pandas) – generování syntetických dat a ETL.
- **SQL Server** – hvězdicové schéma, views, window functions.
- **SQLAlchemy** – připojení Pythonu k databázi.
- **Power BI** – vizualizace klíčových metrik.

## Struktura

- `init_db.sql` – vytvoření databáze a tabulek (dimenze + fakta).
- `data_generator.py` – naplnění databáze syntetickými daty.
- `analytics.sql`, `advanced_queries.sql` – analytické views a dotazy.
- `deploy_analytics.py` – nasazení views a export CSV pro BI.
- `bi_exports/` – exportovaná data.
- `StreamWise_Dashboard.pbix` – report v Power BI.

## Spuštění

1. V SQL Serveru (lokálně `localhost\SQLEXPRESS`) spusťte `init_db.sql`.
2. `pip install pandas sqlalchemy pyodbc`
3. `python data_generator.py`
4. `python deploy_analytics.py`
5. Otevřete `StreamWise_Dashboard.pbix` v Power BI Desktop.

Autor: Tereza Vačina
