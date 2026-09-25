-- Vytvoření nové databáze pro projekt
CREATE DATABASE StreamWiseDB;
GO

USE StreamWiseDB;
GO

-- 1. Dimenze obsahu
DROP TABLE IF EXISTS fact_Engagement; -- nejdřív fakta kvůli FK
DROP TABLE IF EXISTS dim_MediaContent;
CREATE TABLE dim_MediaContent (
    ContentID INT PRIMARY KEY IDENTITY(1,1),
    Title NVARCHAR(200),
    Category NVARCHAR(50), -- Series, Movie, Sport, News
    SubGenre NVARCHAR(50), -- Thriller, Comedy, Romance...
    ProductionCost_USD INT, -- Pro ROI analýzu
    Rating_IMDB DECIMAL(3,1),
    TargetAudience NVARCHAR(20) -- Kids, Adults, General
);

-- 2. Dimenze zařízení
CREATE TABLE dim_Devices (
    DeviceID INT PRIMARY KEY IDENTITY(1,1),
    Platform NVARCHAR(50), -- Android, iOS, WebOS, Tizen
    AppVersion NVARCHAR(20),
    ConnectionType NVARCHAR(20) -- WiFi, 4G, 5G, Ethernet
);

-- 3. Dimenze uživatelů
DROP TABLE IF EXISTS dim_UserBase;
CREATE TABLE dim_UserBase (
    UserID INT PRIMARY KEY IDENTITY(1,1),
    AgeGroup NVARCHAR(20),
    Gender CHAR(1),
    Tier NVARCHAR(20), -- Free, Voyo (No Ads), Premium
    Country NVARCHAR(50),
    AcquisitionSource NVARCHAR(50) -- Facebook, Google, Direct
);

-- 4. Faktová tabulka: Engagement
CREATE TABLE fact_Engagement (
    EngagementID BIGINT PRIMARY KEY IDENTITY(1,1),
    UserID INT FOREIGN KEY REFERENCES dim_UserBase(UserID),
    ContentID INT FOREIGN KEY REFERENCES dim_MediaContent(ContentID),
    DeviceID INT FOREIGN KEY REFERENCES dim_Devices(DeviceID),
    StreamStartTimestamp DATETIME,
    WatchTimeMinutes INT,
    IsInterrupted BIT, -- přerušeno před koncem
    BufferingEvents INT -- vstup pro churn model
);

-- 5. Faktová tabulka: AdImpressions
CREATE TABLE fact_AdImpressions (
    AdID INT PRIMARY KEY IDENTITY(1,1),
    EngagementID BIGINT FOREIGN KEY REFERENCES fact_Engagement(EngagementID),
    AdLengthSeconds INT,
    WasSkipped BIT,
    Revenue_USD DECIMAL(10,4)
);
GO