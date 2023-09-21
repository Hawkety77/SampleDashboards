DROP TABLE IF EXISTS #aggregated_table
		            , #filler_table
                    , #extra_filtered_logs
                    , #joined_table_2
                    , #aggregated_table
                    , #grouping;
DECLARE @cutoff_year_term INT;
SET @cutoff_year_term = (SELECT * FROM SAASPRD.ODS.CurrentYearTerm) - 50;

WITH FilteredLogs AS (
    SELECT byu_id
        , year_term
        , curriculum_id
        , title_code
        , section_number
        , action
        , updated_date_time
        , updated_date
    FROM SAASPRD.STDREG.StdClassLog
    WHERE success = 'Y'
        AND year_term >= @cutoff_year_term
        AND (action = 'A' OR action = 'D')
)
, AggregatedLogs AS (
    SELECT a.byu_id,
        a.year_term,
        a.curriculum_id,
        a.title_code,
        a.section_number,
        a.action,
        a.updated_date_time,
        a.updated_date,
        b.updated_date_time AS end_date_time,
        MIN(b.updated_date_time) OVER (PARTITION BY b.byu_id
                                                    , b.year_term
                                                    , b.curriculum_id
                                                    , b.title_code
                                                    , b.section_number) AS min_end_date_time
    FROM FilteredLogs a
    LEFT JOIN FilteredLogs b
        ON a.byu_id = b.byu_id
        AND a.year_term = b.year_term
        AND a.curriculum_id = b.curriculum_id
        AND a.title_code = b.title_code
        AND a.section_number = b.section_number
        AND a.updated_date_time < b.updated_date_time
        AND a.action = 'A'
        AND b.action = 'D'
)
, ExtraFilteredLogs AS (
    SELECT byu_id
    , a.year_term
    , curriculum_id
    , title_code
    , section_number
    , updated_date AS added_date
    , updated_date_time AS added_date_time
    , (CASE 
        WHEN CONVERT(date, end_date_time) IS NULL THEN DATEADD(week, 2, b.start_date)
        ELSE CONVERT(date, end_date_time)
        END) AS drop_date
    , (CASE 
        WHEN end_date_time IS NULL THEN CONVERT(datetime, DATEADD(week, 2, b.start_date))
        ELSE end_date_time
        END) AS drop_date_time
    , DATEADD(hour, 6, CONVERT(datetime, b.start_date)) AS start_date_time
    FROM AggregatedLogs a
    JOIN SAASPRD.ODS.YearTermExt b 
        ON a.year_term = b.year_term
    WHERE end_date_time = min_end_date_time OR end_date_time IS NULL
        AND action = 'A'
)

SELECT *
INTO #extra_filtered_logs
FROM ExtraFilteredLogs;

-----------------Waitlist Tables--------------------

WITH FilteredTable AS (
    SELECT byu_id
        , year_term
        , curriculum_id
        , title_code
        , section_number
        , [action]
        , action_date_time
        , action_date
    FROM SAASPRD.CLSSCHED.WaitlistLog a
    WHERE year_term >= @cutoff_year_term
)

, JoinedTable AS (
    SELECT a.byu_id
        , a.year_term
        , a.curriculum_id
        , a.title_code
        , a.section_number
        , a.action_date AS insert_date
        , a.action_date_time AS insert_date_time
        , b.action_date_time AS remove_date_time
        , MIN(b.action_date_time)  OVER (PARTITION BY b.byu_id
                                                    , b.year_term
                                                    , b.curriculum_id
                                                    , b.title_code
                                                    , b.section_number) AS min_remove_date_time
        , start_date

    FROM FilteredTable a
    LEFT JOIN FilteredTable b 
        ON a.byu_id = b.byu_id
        AND a.year_term = b.year_term
        AND a.curriculum_id = b.curriculum_id
        AND a.title_code = b.title_code
        AND a.section_number = b. section_number
        AND a.action = 'WL Insert'
        AND b.action <> 'WL Insert'
        AND a.action_date_time < b.action_date_time
    LEFT JOIN SAASPRD.ODS.YearTermExt c
        ON a.year_term = c.year_term
    WHERE a.action = 'WL Insert'
)

, JoinedTable2 AS (
    SELECT a.byu_id
        , a.year_term
        , a.curriculum_id
        , a.title_code
        , a.section_number
        , insert_date
        , insert_date_time
        , (CASE
            WHEN remove_date_time IS NULL THEN DATEADD(week, 2, a.start_date)
            ELSE CONVERT(date, remove_date_time)
            END) AS remove_date
        , (CASE
            WHEN remove_date_time IS NULL THEN CONVERT(datetime, DATEADD(week, 2, a.start_date))
            ELSE remove_date_time
            END) AS remove_date_time
        , DATEADD(hour, 6, CONVERT(datetime, start_date)) AS start_date_time
    FROM JoinedTable a
    WHERE remove_date_time = min_remove_date_time OR min_remove_date_time IS NULL
)

SELECT *
INTO #joined_table_2
FROM JoinedTable2;

WITH CrossJoinedTable AS (
    SELECT DISTINCT a.byu_id
        , a.year_term 
        , a.curriculum_id
        , b.full_date AS active_date
    FROM #joined_table_2 a 
    LEFT JOIN SAASPRD.SAAS.DateDim b 
        ON b.full_date BETWEEN a.insert_date AND a.remove_date
)
----------Combining----------------

, ActualLog AS (
    SELECT DISTINCT byu_id
        , a.year_term
        , curriculum_id
        , b.full_date AS active_date
    FROM #extra_filtered_logs a
    JOIN SAASPRD.SAAS.DateDim b
        ON b.full_date BETWEEN a.added_date AND a.drop_date
)
, CombinedTable AS (
    SELECT  COALESCE(a.year_term, b.year_term) AS year_term 
        , COALESCE(a.curriculum_id, b.curriculum_id) AS curriculum_id
        , COALESCE(a.active_date, b.active_date) AS active_date
        , a.active_date AS enrollment_active_date
        , (CASE 
            WHEN b.active_date IS NOT NULL AND a.active_date IS NULL THEN 1
            ELSE 0
            END) AS not_registered_other_section
    FROM ActualLog a 
    FULL JOIN CrossJoinedTable b
        ON a.byu_id = b.byu_id
        AND a.curriculum_id = b.curriculum_id
        AND a.active_date = b.active_date
)

, AggregatedTable AS (
    SELECT year_term
        , curriculum_id
        , active_date
        , COUNT(enrollment_active_date) AS total_enrollment_by_day
        , SUM(not_registered_other_section) AS total_waitlist_by_day
    FROM CombinedTable

    GROUP BY year_term
            , curriculum_id
            , active_date
)

SELECT *
INTO #aggregated_table
FROM AggregatedTable;

WITH TotalEnrollmentTable AS (
    SELECT byu_id
        , year_term 
        , curriculum_id 
        , (CASE 
            WHEN SUM((CASE 
                        WHEN start_date_time BETWEEN added_date_time AND drop_date_time THEN 1
                        ELSE 0
                        END)) > 0 THEN 1
            ELSE 0
            END) AS enrolled_6am
    FROM #extra_filtered_logs 
    GROUP BY byu_id 
            , year_term
            , curriculum_id
)

, TotalWaitlistTable AS (
    SELECT byu_id
        , year_term 
        , curriculum_id 
        , (CASE 
            WHEN SUM((CASE 
                        WHEN start_date_time BETWEEN insert_date_time AND remove_date_time THEN 1
                        ELSE 0
                        END)) > 0 THEN 1
            ELSE 0
            END) AS enlisted_6am
    FROM #joined_table_2
    GROUP BY byu_id 
            , year_term
            , curriculum_id
)

, Grouping AS (
SELECT COALESCE(a.year_term, b.year_term) AS year_term
    , COALESCE(a.curriculum_id, b.curriculum_id) AS curriculum_id
    , enrolled_6am
    , enlisted_6am
    , (CASE 
        WHEN enlisted_6am = 1 AND enrolled_6am = 1 THEN 0
        ELSE enlisted_6am
        END) AS total_waitlist
FROM TotalEnrollmentTable a
FULL JOIN TotalWaitlistTable b
    ON a.year_term = b.year_term
    AND a.curriculum_id = b.curriculum_id 
    AND a.byu_id = b.byu_id
)

SELECT *
INTO #grouping
FROM Grouping;

WITH FillerTablePrep AS (

    SELECT DISTINCT a.year_term 
                    , a.curriculum_id
                    , b.start_date
    FROM SAASPRD.ODS.ClassSectionExt a 
    LEFT JOIN SAASPRD.ODS.YearTermExt b 
        ON a.year_term = b.year_term
    WHERE a.year_term > @cutoff_year_term
) 

, FillerTable AS (
    SELECT a.year_term
        , a.curriculum_id
        , b.full_date AS active_date
        , a.start_date
    FROM FillerTablePrep a
    JOIN SAASPRD.SAAS.DateDim b
        ON b.full_date BETWEEN DATEADD(week, 2, DATEADD(year, -1, a.start_date)) AND DATEADD(week, 2, a.start_date)
)

SELECT *
INTO #filler_table
FROM FillerTable;


-------------CUSTOM SQL ENROLLMENT & WAITLIST---------------

SELECT a.year_term 
    , a.curriculum_id
    , a.active_date 
    , (CASE
        WHEN b.total_enrollment_by_day IS NULL THEN 0
        ELSE b.total_enrollment_by_day
        END) AS total_enrollment_by_day
    , (CASE
        WHEN b.total_waitlist_by_day IS NULL THEN 0
        ELSE b.total_waitlist_by_day
        END) AS total_waitlist_by_day
FROM #filler_table a
LEFT JOIN #aggregated_table b 
    ON a.year_term = b.year_term
    AND a.curriculum_id = b.curriculum_id
    AND a.active_date = b.active_date
WHERE a.active_date BETWEEN DATEADD(week, 2, DATEADD(year, -1, a.start_date)) AND DATEADD(week, 2, a.start_date)
