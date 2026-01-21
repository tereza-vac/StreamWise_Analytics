# 📺 StreamWise Analytics: End-to-End Data Engineering Project

## O projektu
Tento projekt simuluje práci Data Engineera pro streamovací platformu (jako Voyo, Netflix). Cílem bylo vytvořit kompletní datovou pipelinu od generování dat až po finální business reporting a přípravu features pro AI modely.

Projekt řeší reálné problémy:
* **Analýza sledovanosti** (Prime Time, Revenue).
* **Segmentace zákazníků** (Free vs. Premium).
* **AI Feature Engineering** (Predikce odchodu uživatelů na základě technických chyb).

## Tech Stack
* **Python (ETL):** Generování syntetických dat, čištění a transformace (Pandas).
* **SQL Server (Data Warehousing):** Návrh hvězdicového schématu (Star Schema), Views, Window Functions.
* **SQLAlchemy:** ORM pro komunikaci Pythonu s databází.
* **Power BI:** Vizualizace klíčových metrik a reporting.

## Struktura projektu
* `data_generator.py` - ETL skript pro generování dat a plnění DB.
* `deploy_analytics.py` - Nasazení SQL Views a automatický export dat pro BI.
* `StreamWise_Dashboard.pbix` - Interaktivní report (Power BI).
* `bi_exports/` - CSV soubory připravené pro reporting.

## Ukázka vizualizace
Projekt obsahuje analýzu, která odhaluje korelaci mezi věrností uživatele a technickými chybami (Buffering), což slouží jako vstup pro Churn Prediction Model.

---
*Autor: Tereza Vačina*# StreamWise_Analytics
This project demonstrates an end-to-end data pipeline for a modern streaming platform. It includes synthetic data generation, relational modeling (Star Schema), and preparation of analytical layers for Machine Learning models.
