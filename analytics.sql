USE StreamWiseDB;
GO

-- =========================================================
-- 1. VIEW: ANALÝZA KVALITY OBSAHU (Content Performance)
-- =========================================================
-- Business otázka: Které pořady "táhnou" a které lidé vypínají?
-- Používáme Window Functions (DENSE_RANK) pro žebříčky.

CREATE OR ALTER VIEW v_ContentPerformance AS
SELECT 
    c.Title,
    c.Category,
    c.TargetAudience,
    COUNT(e.EngagementID) AS TotalViews,
    
    -- Metrika: Completion Rate (Kolik % lidí to dokoukalo?)
    -- Pokud je pod 50 %, pořad je propadák.
    FORMAT(SUM(CAST(e.IsInterrupted AS INT)) * 1.0 / COUNT(e.EngagementID), 'P') AS DropOffRate,
    
    -- Window Function: Pořadí v rámci kategorie
    -- Toto ukazuje, že umíš pokročilé SQL (nejen GROUP BY)
    DENSE_RANK() OVER (PARTITION BY c.Category ORDER BY COUNT(e.EngagementID) DESC) as CategoryRank

FROM fact_Engagement e
JOIN dim_MediaContent c ON e.ContentID = c.ContentID
GROUP BY c.Title, c.Category, c.TargetAudience;
GO

-- =========================================================
-- 2. VIEW: FEATURE STORE PRO AI (Churn Prediction)
-- =========================================================
-- Data Science tým potřebuje tabulku "jeden řádek = jeden uživatel".
-- Zde připravujeme "Features" (vstupy) pro model, který hledá nespokojené lidi.

CREATE OR ALTER VIEW v_UserChurnFeatures AS
SELECT 
    u.UserID,
    u.AgeGroup,
    u.Tier,
    
    -- Feature 1: Engagement (Jak moc sleduje?)
    COUNT(e.EngagementID) as TotalSessions,
    SUM(e.WatchTimeMinutes) as TotalMinutesWatched,
    
    -- Feature 2: Tech Experience (Sekalo se mu to?)
    -- Pokud má uživatel hodně bufferingu, pravděpodobně odejde ke konkurenci.
    SUM(e.BufferingEvents) as TotalBufferingEvents,
    
    -- Feature 3: Preference (Co má rád?)
    -- Jednoduchá logika: Který žánr viděl naposledy
    MAX(c.Category) as LastWatchedCategory

FROM dim_UserBase u
LEFT JOIN fact_Engagement e ON u.UserID = e.UserID
LEFT JOIN dim_MediaContent c ON e.ContentID = c.ContentID
GROUP BY u.UserID, u.AgeGroup, u.Tier;
GO