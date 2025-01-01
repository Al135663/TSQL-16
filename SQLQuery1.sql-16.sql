--Aggregating Data with Group By

CREATE PROCEDURE GetRevenueByGenre @CountryID INT = NULL
AS
BEGIN
    -- Validate CountryID
    IF @CountryID IS NOT NULL AND NOT EXISTS (SELECT 1 FROM production_country WHERE country_id = @CountryID)
    BEGIN
        RAISERROR('Invalid country ID provided.', 16, 1);
        RETURN;
    END

    -- Query to calculate total revenue by genre
    SELECT 
        g.genre_name,
        SUM(m.revenue) AS TotalRevenue
    FROM 
        movie AS m
    JOIN 
        movie_genres AS mg ON m.movie_id = mg.movie_id
    JOIN 
        genre AS g ON mg.genre_id = g.genre_id
    LEFT JOIN 
        production_country AS pc ON m.movie_id = pc.movie_id
    WHERE 
        (@CountryID IS NULL OR pc.country_id = @CountryID)
    GROUP BY 
        g.genre_name
    ORDER BY 
        TotalRevenue DESC;
END;
GO

-------  Using Table-Valued Functions (TVFs) for Modularization

DROP PROCEDURE GetRevenueByGenre;
GO

-- TSQL16 Demo 2: Using Table-Valued Functions (TVFs) for Modularization
CREATE FUNCTION dbo.GetRevenueByGenre(@CountryID INT = NULL)
RETURNS TABLE
AS
RETURN (
    SELECT 
        g.genre_name,
        SUM(m.revenue) AS TotalRevenue
    FROM 
        movie AS m
    INNER JOIN 
        movie_genres AS mg ON m.movie_id = mg.movie_id
    INNER JOIN 
        genre AS g ON mg.genre_id = g.genre_id
    LEFT JOIN 
        production_country AS pc ON m.movie_id = pc.movie_id
    WHERE 
        (@CountryID IS NULL OR pc.country_id = @CountryID)
    GROUP BY 
        g.genre_name
);
GO

-- Create the procedure to fetch genre revenue metrics
CREATE PROCEDURE dbo.FetchGenreRevenueMetrics
    @CountryID INT = NULL
AS
BEGIN
    -- Validate input parameter for CountryID
    IF @CountryID IS NOT NULL AND NOT EXISTS (SELECT 1 FROM country WHERE country_id = @CountryID)
    BEGIN
        RAISERROR('Invalid Country ID provided. Please enter a valid country ID.', 16, 1);
        RETURN;
    END

    -- Execute the TVF
    SELECT * FROM dbo.GetRevenueByGenre(@CountryID);
END;
GO

-- Executing the Procedure Without a Country Filter
EXEC dbo.FetchGenreRevenueMetrics;

-- Execute the Procedure With a Country ID
EXEC dbo.FetchGenreRevenueMetrics @CountryID = 131;


---Control-of-Flow with Window Functions

CREATE PROCEDURE AnalyzeGenreTrends
    @GenreID INT
AS
BEGIN
    -- Validate the existence of the GenreID
    IF NOT EXISTS (SELECT 1 FROM genre WHERE genre_id = @GenreID)
    BEGIN
        RAISERROR('Invalid Genre ID provided. Please enter a valid genre ID.', 10, 800);
        RETURN;
    END

    -- Query to analyze cumulative revenue and ranking of movies within the specified genre
    ;WITH RevenueRanking AS (
        SELECT
            m.title,
            m.revenue,
            SUM(m.revenue) OVER (ORDER BY m.release_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS CumulativeRevenue,
            RANK() OVER (ORDER BY m.revenue DESC) AS RevenueRank
        FROM movie AS m
        JOIN movie_genres AS mg ON m.movie_id = mg.movie_id
        WHERE mg.genre_id = @GenreID
    )
    SELECT
        title,
        revenue,
        CumulativeRevenue,
        RevenueRank
    FROM RevenueRanking
    ORDER BY RevenueRank;

    -- Check if any data was returned
    IF @@ROWCOUNT = 0
    BEGIN
        PRINT 'No data available for the specified genre.';
    END
END;
GO


EXEC AnalyzeGenreTrends @GenreID = 14;


-----Pivoting Data for Analysis
CREATE PROCEDURE dbo.PivotRevenueByGenreAndCountry
    @GenreID INT = NULL, -- filter for genre
    @CountryID INT = NULL -- filter for country
AS
BEGIN
    -- Declare a table variable to hold intermediate revenue data
    DECLARE @RevenueData TABLE (
        GenreName NVARCHAR(100),
        CountryName NVARCHAR(100),
        TotalRevenue DECIMAL(18, 2)
    );

    -- Insert data into the table variable
    INSERT INTO @RevenueData (GenreName, CountryName, TotalRevenue)
    SELECT 
        g.genre_name AS GenreName,
        c.country_name AS CountryName,
        SUM(m.revenue) AS TotalRevenue
    FROM 
        movie AS m
    INNER JOIN 
        movie_genres AS mg ON m.movie_id = mg.movie_id
    INNER JOIN 
        genre AS g ON mg.genre_id = g.genre_id
    INNER JOIN 
        production_country AS pc ON m.movie_id = pc.movie_id
    INNER JOIN 
        country AS c ON pc.country_id = c.country_id
    WHERE 
        (@GenreID IS NULL OR g.genre_id = @GenreID) AND
        (@CountryID IS NULL OR c.country_id = @CountryID)
    GROUP BY 
        g.genre_name, c.country_name;

    -- Performing pivot operation
    SELECT 
        GenreName,
        [USA], [UK], [Canada], [Germany], [France], [Japan]
    FROM 
        (SELECT GenreName, CountryName, TotalRevenue FROM @RevenueData) AS SourceData
    PIVOT 
        (SUM(TotalRevenue)
        FOR CountryName IN ([USA], [UK], [Canada], [Germany], [France], [Japan])
        ) AS PivotedData;
END;
GO

-- Test without filters
EXEC dbo.PivotRevenueByGenreAndCountry;

-- Test with specific genre and country filters
EXEC dbo.PivotRevenueByGenreAndCountry @GenreID = 28, @CountryID = 128;



--Comprehensive Analysis with Cursors and User-Defined Functions.
-- Create an ErrorLog table for logging errors
CREATE TABLE dbo.ErrorLog (
    ErrorID INT IDENTITY(1,1) PRIMARY KEY,
    ErrorDateTime DATETIME NOT NULL DEFAULT GETDATE(),
    ErrorMessage NVARCHAR(1000),
    ErrorSeverity INT,
    ErrorState INT
);
GO

-- Creating a User-Defined Function
IF OBJECT_ID('dbo.GetAverageRevenue', 'FN') IS NOT NULL
    DROP FUNCTION dbo.GetAverageRevenue;
GO

CREATE FUNCTION dbo.GetAverageRevenue(@GenreID INT)
RETURNS DECIMAL(18, 2)
AS
BEGIN
    DECLARE @AverageRevenue DECIMAL(18, 2);

    SELECT @AverageRevenue = AVG(m.revenue)
    FROM movie AS m
    INNER JOIN movie_genres AS mg ON m.movie_id = mg.movie_id
    WHERE mg.genre_id = @GenreID;

    RETURN @AverageRevenue;
END;
GO

-- Creating a Stored Procedure
IF OBJECT_ID('dbo.AnalyzeGenreRevenues', 'P') IS NOT NULL
    DROP PROCEDURE dbo.AnalyzeGenreRevenues;
GO

CREATE PROCEDURE dbo.AnalyzeGenreRevenues
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @GenreID INT, @GenreName NVARCHAR(100), @AvgRevenue DECIMAL(18, 2);

    BEGIN TRY
        -- Define a cursor to iterate over genres with high-revenue movies
        DECLARE genre_cursor CURSOR FOR
        SELECT genre_id, genre_name
        FROM genre
        WHERE genre_id IN (
            SELECT genre_id
            FROM movie_genres
            WHERE movie_id IN (
                SELECT movie_id
                FROM movie
                WHERE revenue > 1000000
            )
        );

        -- Open the cursor and fetch the first row
        OPEN genre_cursor;
        FETCH NEXT FROM genre_cursor INTO @GenreID, @GenreName;

        WHILE @@FETCH_STATUS = 0
        BEGIN
            BEGIN TRY
                -- Use the user-defined function to calculate average revenue
                SET @AvgRevenue = dbo.GetAverageRevenue(@GenreID);
                PRINT 'Genre: ' + @GenreName + ', Average Revenue: ' + CAST(@AvgRevenue AS NVARCHAR(50));
            END TRY
            BEGIN CATCH
                -- Log errors for each genre processing failure
                INSERT INTO ErrorLog(ErrorMessage, ErrorSeverity, ErrorState)
                VALUES(ERROR_MESSAGE(), ERROR_SEVERITY(), ERROR_STATE());
            END CATCH;

            FETCH NEXT FROM genre_cursor INTO @GenreID, @GenreName;
        END;

        -- Close and deallocate the cursor
        CLOSE genre_cursor;
        DEALLOCATE genre_cursor;
    END TRY
    BEGIN CATCH
        -- Log top-level errors
        INSERT INTO ErrorLog(ErrorMessage, ErrorSeverity, ErrorState)
        VALUES(ERROR_MESSAGE(), ERROR_SEVERITY(), ERROR_STATE());
    END CATCH;
END;
GO

-- Execute the Stored Procedure
EXEC dbo.AnalyzeGenreRevenues;