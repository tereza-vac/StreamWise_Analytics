USE StreamWiseDB;
GO

-- =========================================================
-- Demografie vs. oblíbené žánry
-- =========================================================

SELECT 
    u.AgeGroup,
    u.Gender,
    c.Category,
    
    COUNT(e.EngagementID) AS TotalViews,
    SUM(e.WatchTimeMinutes) AS TotalTimeSpent,
    ROUND(AVG(e.WatchTimeMinutes), 1) AS AvgSession,
    
    CASE 
        WHEN AVG(e.WatchTimeMinutes) > 40 THEN 'High Engagement'
        WHEN AVG(e.WatchTimeMinutes) > 20 THEN 'Medium Engagement'
        ELSE 'Low Engagement'
    END AS EngagementLevel

FROM dim_UserBase u
-- LEFT JOIN: i uživatelé bez sledování
LEFT JOIN fact_Engagement e ON u.UserID = e.UserID

LEFT JOIN dim_MediaContent c ON e.ContentID = c.ContentID

GROUP BY u.AgeGroup, u.Gender, c.Category

-- Jen uživatelé, kteří něco viděli
HAVING COUNT(e.EngagementID) > 0

ORDER BY TotalViews DESC;
GO