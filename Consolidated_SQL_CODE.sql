

/** STEP 1 **/
/** Text file import of data to cloud DB **/
/** We used the SQL Server 2016 Import and Export Data wizard  **/


/** Alternatively use bulk import unfortunately bulk command is not acceptable on AWS cloud DB **/

CREATE TABLE EAV(
Patient int,
Question int,
Score int
)
BULK INSERT EAV
FROM 'C:\Biomedical_data\datafile.txt' 
WITH
(
FIELDTERMINATOR = '\t',
ROWTERMINATOR = '\n'
)
SELECT * FROM EAV

/** STEP 2 **/
-- To allow for indexing, we first changed the datatype from varchar to int

/*** Indexing - grouping of equities for query optimization of large data set ***/

DECLARE @StartTime datetime
DECLARE @EndTime datetime
SELECT @StartTime=GETDATE()

CREATE INDEX ID_QN_INDEX ON EAV (Question, Patient);

SELECT @EndTime=GETDATE()
 
--This will return execution time of your query
SELECT DATEDIFF(mS,@StartTime,@EndTime) AS [Duration in millisecs]

/** TO DROP INDEX table_name.index_name;**/
DROP INDEX EAV.ID_QN_INDEX;




/** STEP 3 **/
/** Table tranformation using SQL inbuilt PIVOT command **/

DECLARE @StartTime datetime
DECLARE @EndTime datetime
SELECT @StartTime=GETDATE()

select * INTO cPivot
from 
(
  select Patient, Question, Score
  from EAV
) src
pivot
(
  sum(Score)
  for Question in ([1], [2], [3], [4], [5], [6], [7], [8], [9], [10],
					[11], [12], [13], [14], [15], [16], [17], [18], [19], [20],
					[21], [22], [23], [24], [25], [26], [27], [28], [29], [30],
					[31], [32], [33], [34], [35], [36], [37], [38], [39], [40],
					[41], [42])
) piv

ORDER BY Patient;
SELECT @EndTime=GETDATE()
 
--This will return execution time of the query in milliseconds as specified
SELECT DATEDIFF(mS,@StartTime,@EndTime) AS [Duration in millisecs]



/** STEP 4 **/
/** Table tranformation using left join **/
/***
LEFT OUTER JOIN METHOD:
1. Create a table of unique entities.
Select distinct [Entity column name] into [Unique Entities table] from [eav_Table]
2. Select [unique entities].[entity column name], attribute_1, attribute_2, …attribute_N
From [unique entities] as U
left join
(select [entity], [Value] as Attrribute_1 from EAV_Table where Attribute=1) as M1 on U.Entity = M1.entity
Left join
(select [entity], [Value] as Attrribute_2 from EAV_Table where Attribute=2) as M2 on U.Entity = M2.entity
….
(until attribute 10)
***/
/*** LEFT OUTER JOIN METHOD: ***/
SELECT DISTINCT [Patient] INTO UniqEnt FROM EAV

DECLARE @StartTime datetime
DECLARE @EndTime datetime
SELECT @StartTime=GETDATE()

SELECT U.[Patient], [Question 1 Score], [Question 2 Score] , [Question 3 Score] , [Question 4 Score] , 
 [Question 5 Score] , [Question 6 Score] , [Question 7 Score] , [Question 8 Score] ,
 [Question 9 Score] , [Question 10 Score] , [Question 11 Score] , [Question 12 Score] ,
 [Question 13 Score] , [Question 14 Score] , [Question 15 Score] , [Question 16 Score] ,
 [Question 17 Score] , [Question 18 Score] , [Question 19 Score] , [Question 20 Score] ,
 [Question 21 Score] , [Question 22 Score] , [Question 23 Score] , [Question 24 Score] ,
 [Question 25 Score] , [Question 26 Score] , [Question 27 Score] , [Question 28 Score] ,
 [Question 29 Score] , [Question 30 Score] , [Question 31 Score] , [Question 32 Score] ,
 [Question 33 Score] , [Question 34 Score] , [Question 35 Score] , [Question 36 Score] ,
 [Question 37 Score] , [Question 38 Score] , [Question 39 Score] , [Question 40 Score] ,
 [Question 41 Score] , [Question 42 Score] INTO LPivot FROM UniqEnt AS U 
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 1 Score' FROM EAV WHERE Question=1) AS  M1 ON U.Patient = M1.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 2 Score' FROM EAV WHERE Question=2) AS M2 ON U.Patient = M2.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 3 Score' FROM EAV WHERE Question=3) AS  M3 ON U.Patient = M3.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 4 Score' FROM EAV WHERE Question=4) AS M4 ON U.Patient = M4.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 5 Score' FROM EAV WHERE Question=5) AS  M5 ON U.Patient = M5.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 6 Score' FROM EAV WHERE Question=6) AS M6 ON U.Patient = M6.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 7 Score' FROM EAV WHERE Question=7) AS  M7 ON U.Patient = M7.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 8 Score' FROM EAV WHERE Question=8) AS M8 ON U.Patient = M8.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 9 Score' FROM EAV WHERE Question=9) AS  M9 ON U.Patient = M9.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 10 Score' FROM EAV WHERE Question=10) AS M10 ON U.Patient = M10.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 11 Score' FROM EAV WHERE Question=11) AS  M11 ON U.Patient = M11.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 12 Score' FROM EAV WHERE Question=12) AS M12 ON U.Patient = M12.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 13 Score' FROM EAV WHERE Question=13) AS  M13 ON U.Patient = M13.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 14 Score' FROM EAV WHERE Question=14) AS M14 ON U.Patient = M14.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 15 Score' FROM EAV WHERE Question=15) AS  M15 ON U.Patient = M15.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 16 Score' FROM EAV WHERE Question=16) AS M16 ON U.Patient = M16.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 17 Score' FROM EAV WHERE Question=17) AS  M17 ON U.Patient = M17.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 18 Score' FROM EAV WHERE Question=18) AS M18 ON U.Patient = M18.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 19 Score' FROM EAV WHERE Question=19) AS  M19 ON U.Patient = M19.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 20 Score' FROM EAV WHERE Question=20) AS M20 ON U.Patient = M20.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 21 Score' FROM EAV WHERE Question=21) AS  M21 ON U.Patient = M21.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 22 Score' FROM EAV WHERE Question=22) AS M22 ON U.Patient = M22.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 23 Score' FROM EAV WHERE Question=23) AS  M23 ON U.Patient = M23.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 24 Score' FROM EAV WHERE Question=24) AS M24 ON U.Patient = M24.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 25 Score' FROM EAV WHERE Question=25) AS  M25 ON U.Patient = M25.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 26 Score' FROM EAV WHERE Question=26) AS M26 ON U.Patient = M26.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 27 Score' FROM EAV WHERE Question=27) AS  M27 ON U.Patient = M27.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 28 Score' FROM EAV WHERE Question=28) AS M28 ON U.Patient = M28.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 29 Score' FROM EAV WHERE Question=29) AS  M29 ON U.Patient = M29.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 30 Score' FROM EAV WHERE Question=30) AS M30 ON U.Patient = M30.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 31 Score' FROM EAV WHERE Question=31) AS  M31 ON U.Patient = M31.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 32 Score' FROM EAV WHERE Question=32) AS M32 ON U.Patient = M32.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 33 Score' FROM EAV WHERE Question=33) AS  M33 ON U.Patient = M33.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 34 Score' FROM EAV WHERE Question=34) AS M34 ON U.Patient = M34.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 35 Score' FROM EAV WHERE Question=35) AS  M35 ON U.Patient = M35.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 36 Score' FROM EAV WHERE Question=36) AS M36 ON U.Patient = M36.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 37 Score' FROM EAV WHERE Question=37) AS  M37 ON U.Patient = M37.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 38 Score' FROM EAV WHERE Question=38) AS M38 ON U.Patient = M38.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 39 Score' FROM EAV WHERE Question=39) AS  M39 ON U.Patient = M39.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 40 Score' FROM EAV WHERE Question=40) AS M40 ON U.Patient = M40.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 41 Score' FROM EAV WHERE Question=41) AS  M41 ON U.Patient = M41.Patient
LEFT JOIN
(SELECT [Patient], [Score] AS 'Question 42 Score' FROM EAV WHERE Question=42) AS M42 ON U.Patient = M42.Patient

SELECT @EndTime=GETDATE()
 
--This will return execution time of the query
SELECT DATEDIFF(mS,@StartTime,@EndTime) AS [Duration in millisecs]


/** STEP 5 **/
-- Because of storage size challenges, we reduced the data size 
-- We then repeated the above steps on a new database instance with higher storage capacity

select max(Patient) from newTable  /* = 2000000 */

 /***insert into NewTable select [Entity column Name]], [Attribute_column Name],[Value column Name]
 from [your EAV table name] where [Entity column Name] > [count of entities/10] ***/

 insert into NewDataF select [Patient], [Question],[Score]
 from newTable where [Patient] < 200000
 
 
 
 
 --Investigating query performance before and after Indexing
 
/*** Execution time before Indexing  ***/
DECLARE @StartTime datetime
DECLARE @EndTime datetime
SELECT @StartTime=GETDATE()

SELECT * FROM EAV WHERE Patient=12 AND Score = 5;

SELECT @EndTime=GETDATE()

--This will return execution time of your query
SELECT DATEDIFF(mS,@StartTime,@EndTime) AS [Duration in millisecs]

--Time1 = 540 millisecs
--Time2 = 417 millisecs
--Time3 = 410 millisecs
-- (4 row(s) affected)


/*** Execution time after Indexing  ***/

create unique clustered index ID_QN_INDEX on EAV (Question, Patient);

DECLARE @StartTime datetime
DECLARE @EndTime datetime
SELECT @StartTime=GETDATE()

SELECT * FROM EAV WHERE Patient=12 AND Score = 5;

SELECT @EndTime=GETDATE()

--This will return execution time of your query
SELECT DATEDIFF(mS,@StartTime,@EndTime) AS [Duration in millisecs]

--Time1 = 397 millisecs
--Time2 = 367 millisecs
--Time3 = 360 millisecs
-- (4 row(s) affected)




--Attribute Centered queries execution time comparison

--A simple attribute-centered query was constructed to compare the execution time between an EAV and a relational table.

/**  1. Query issued on EAV  **/

DECLARE @StartTime datetime
DECLARE @EndTime datetime
SELECT @StartTime=GETDATE()

SELECT * FROM EAV WHERE Question = 16 AND Score=3;

SELECT @EndTime=GETDATE()

--This will return execution time of your query
SELECT DATEDIFF(mS,@StartTime,@EndTime) AS [Duration in millisecs]

--Time1 = 41760 millisecs
--Time2 = 7237 millisecs
--Time3 = 4797 millisecs
--(20034 row(s) affected)

/** 2. Query issued on a conventional table  **/

DECLARE @StartTime datetime
DECLARE @EndTime datetime
SELECT @StartTime=GETDATE()

SELECT Patient, [Question 16 Score] FROM CONVENTIONAL_TABLE WHERE [Question 16 Score]=3;

SELECT @EndTime=GETDATE()

--This will return execution time of your query
SELECT DATEDIFF(mS,@StartTime,@EndTime) AS [Duration in millisecs]

-- Time1 = 4390 millisecs
-- Time2 = 2513 millisecs
-- Time3 = 1680 millisecs
--(20034 row(s) affected)
