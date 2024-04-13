--Create Database

CREATE DATABASE IF NOT EXISTS EmpCompAnalysisDB 
COMMENT 'This database contains details about certification details and completion status'
WITH DBPROPERTIES('creator' = 'rahul')
;

USE EmpCompAnalysisDB ;

--Create two Hive tables for the provided datasets 

CREATE TABLE IF NOT EXISTS CertificationDetails (
CertificationOwner STRING,
CertificationCode STRING,
CertificationTitle STRING,
CompetencyDimension STRING,
Certification_Type STRING,
ComplexityLevel STRING
)
COMMENT 'This table contains the details certifications in the organization'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES('creator' = 'raju')
;

--loading data into CertificationDetails table :
 
LOAD DATA INPATH '/user/Certification_empno/CertificationDetails.csv' INTO TABLE CertificationDetails;

--create table for CDPCompletionStatus 

CREATE TABLE IF NOT EXISTS CDPCompletionStatus(
EmpNo INT,
RoleCapability STRING,
EmpPUCode STRING,
SBUCode STRING,
CertificationCode STRING,
CertificationTitle STRING,
Competency STRING,
Certification_Type STRING,
Certification_Group STRING,
Contact_Based_Program_Y_N STRING,
ExamDate STRING,
Onsite_Offshore STRING,
AttendedStatus STRING,
Marks INT,
Result STRING,
Status STRING,
txtPlanCategory STRING,
SkillD1 INT,
Complexity STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES('creator' ='rahul')
;

--loading data into CDPCompletionStatus table

LOAD DATA INPATH '/user/CDPCompletionStatus_empno/CDPCompletionStatus.csv' INTO TABLE CDPCompletionStatus;

 
--List the count of employees qualified for various certifications ,directly save the file in HDFS

INSERT OVERWRITE DIRECTORY '/user/Hive_Output/employee_count_various_certifications.csv' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
SELECT CertificationTitle,COUNT(empno) AS employee_count
FROM CDPCompletionStatus
WHERE Result = 'Qualified'
GROUP BY CertificationTitle;


--List the certifications available under a specific SBUCode

INSERT OVERWRITE DIRECTORY '/user/Hive_Output/certifications_under_SBUCode.csv' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
SELECT SBUCode,CertificationTitle
FROM CDPCompletionStatus
WHERE SBUCode = 'LT-BIGDATA'
GROUP BY SBUCode,CertificationTitle;

--List the count of employees attempted a particular certification from a specific unit (column: EmpPUCode)

INSERT OVERWRITE DIRECTORY '/user/Hive_Output/certification_specific_unit.csv' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE
SELECT CertificationTitle,EmpPUCode,COUNT(EmpNo) AS employee_count
FROM CDPCompletionStatus
GROUP BY EmpPUCode,CertificationTitle;

--Split the CertificationDetails table into two partitions using dynamic partitioning based on the values of Certification_Type column

CREATE TABLE IF NOT EXISTS CertificationDetails_dp(
CertificationOwner STRING,
CertificationCode STRING,
CertificationTitle STRING,
CompetencyDimension STRING,
ComplexityLevel STRING
)
COMMENT 'This table contains the details of certifications in the organization'
PARTITIONED BY (Certification_Type STRING)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES('creator'= 'rahul')
;

set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE CertificationDetails_dp 
PARTITION (Certification_Type)
SELECT CertificationOwner,CertificationCode,CertificationTitle,CompetencyDimension,ComplexityLevel,Certification_Type
FROM CertificationDetails;


--Distribute CDPCompletionStatus data randomly into 5 buckets based on Marks column


CREATE TABLE IF NOT EXISTS CDPCompletionStatus_Bucket(
EmpNo INT,
RoleCapability STRING,
EmpPUCode STRING,
SBUCode STRING,
CertificationCode STRING,
CertificationTitle STRING,
Competency STRING,
Certification_Type STRING,
Certification_Group STRING,
Contact_Based_Program_Y_N STRING,
ExamDate STRING,
Onsite_Offshore STRING,
AttendedStatus STRING,
Marks INT,
Result STRING,
Status STRING,
txtPlanCategory STRING,
SkillD1 INT,
Complexity STRING
) 
COMMENT 'Bucketed table based on Marks column'
CLUSTERED BY (Marks) INTO 5 buckets
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
TBLPROPERTIES('creator' ='rahul')
;


--Insert data into CDPCompletionStatus_Bucket

INSERT OVERWRITE TABLE CDPCompletionStatus_Bucket
SELECT * FROM CDPCompletionStatus;

--Find the maximum, minimum and average Marks scored by employees in CDPCompletionStatus_Bucket

INSERT OVERWRITE DIRECTORY '/user/Hive_Output/marks_scored.csv' 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE 
SELECT EmpNo,MAX(Marks) AS max_marks,MIN(Marks) AS min_marks,AVG(Marks) AS avg_marks
FROM CDPCompletionStatus_Bucket
GROUP BY EmpNo;






 
