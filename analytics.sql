USE StreamWiseDB;
GO

-- =========================================================
-- 1. VIEW: ANALÝZA KVALITY OBSAHU (Content Performance)
-- =========================================================
-- Sledovanost a completion rate pořadů

CREATE OR ALTER VIEW v_ContentPerformance AS
SELECT 
    c.Title,
    c.Category,
    c.TargetAudience,
    COUNT(e.EngagementID) AS TotalViews,
    
    -- Drop-off rate: podíl přerušených přehrání
    FORMAT(SUM(CAST(e.IsInterrupted AS INT)) * 1.0 / COUNT(e.EngagementID), 'P') AS DropOffRate,
    
    -- Pořadí v rámci kategorie
    DENSE_RANK() OVER (PARTITION BY c.Category ORDER BY COUNT(e.EngagementID) DESC) as CategoryRank

FROM fact_Engagement e
JOIN dim_MediaContent c ON e.ContentID = c.ContentID
GROUP BY c.Title, c.Category, c.TargetAudience;
GO

-- =========================================================
-- 2. VIEW: FEATURE STORE PRO AI (Churn Prediction)
-- =========================================================
-- Jeden řádek = jeden uživatel

CREATE OR ALTER VIEW v_UserChurnFeatures AS
SELECT 
    u.UserID,
    u.AgeGroup,
    u.Tier,
    
    -- Engagement
    COUNT(e.EngagementID) as TotalSessions,
    SUM(e.WatchTimeMinutes) as TotalMinutesWatched,
    
    -- Technická kvalita (buffering)
    SUM(e.BufferingEvents) as TotalBufferingEvents,
    
    -- Naposledy sledovaný žánr
    MAX(c.Category) as LastWatchedCategory

FROM dim_UserBase u
LEFT JOIN fact_Engagement e ON u.UserID = e.UserID
LEFT JOIN dim_MediaContent c ON e.ContentID = c.ContentID
GROUP BY u.UserID, u.AgeGroup, u.Tier;
GO