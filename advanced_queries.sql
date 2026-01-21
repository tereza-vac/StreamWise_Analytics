USE StreamWiseDB;
GO

-- =========================================================
-- POKROČILÁ ANALÝZA: Demografie vs. Oblíbené žánry
-- =========================================================
-- Cíl: Zjistit, jak se liší vkus diváků podle věku a pohlaví.
-- Používáme LEFT JOIN, abychom nevyřadili uživatele, kteří zatím nic neviděli.

SELECT 
    u.AgeGroup,              -- Dimenze: Věková skupina
    u.Gender,                -- Dimenze: Pohlaví
    c.Category,              -- Dimenze: Žánr (např. Reality, Crime)
    
    -- Agregace (GROUP BY metriky)
    COUNT(e.EngagementID) AS TotalViews,             -- Kolikrát si to pustili
    SUM(e.WatchTimeMinutes) AS TotalTimeSpent,       -- Celkový čas strávený u žánru
    ROUND(AVG(e.WatchTimeMinutes), 1) AS AvgSession, -- Průměrná délka jednoho sledování
    
    -- Podmíněné formátování (ukázka logiky v SQL)
    CASE 
        WHEN AVG(e.WatchTimeMinutes) > 40 THEN 'High Engagement'
        WHEN AVG(e.WatchTimeMinutes) > 20 THEN 'Medium Engagement'
        ELSE 'Low Engagement'
    END AS EngagementLevel

FROM dim_UserBase u
-- 1. Připojíme historii sledovanosti (Fakta)
-- Používám LEFT JOIN: Chci vidět uživatele, i když zatím nic nepustili (budou mít NULL hodnoty)
LEFT JOIN fact_Engagement e ON u.UserID = e.UserID

-- 2. Připojíme informace o pořadech (Dimenze)
LEFT JOIN dim_MediaContent c ON e.ContentID = c.ContentID

-- 3. Seskupíme data podle demografie a žánru
GROUP BY u.AgeGroup, u.Gender, c.Category

-- 4. Vyfiltrujeme jen relevantní data (např. zahodíme řádky, kde uživatel nic neviděl)
HAVING COUNT(e.EngagementID) > 0

-- 5. Seřadíme to, aby nahoře byly nejsledovanější kombinace
ORDER BY TotalViews DESC;
GO