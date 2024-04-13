--To Create and load the data into a relation named Rel_CDPCompletionData_empno using the file in CDPCompletionStatus_empno HDFS directory

Rel_CDPCompletionData_empno = LOAD '/user/CDPCompletionStatus_empno/CDPCompletionStatus.csv' USING PigStorage(',') AS 
(
EmpNo:int,
RoleCapability:chararray,
EmpPUCode:chararray,
SBUCode:chararray,
CertificationCode:chararray,
CertificationTitle:chararray,
Competency:chararray,
Certification_Type:chararray,
Certification_Group:chararray,
Contact_Based_Program_Y_N:chararray,
ExamDate:chararray,
Onsite_Offshore:chararray,
AttendedStatus:chararray,
Marks:int,
Result:chararray,
Status:chararray,
txtPlanCategory:chararray,
SkillID1:int,
Complexity:chararray
);

--Store the relation Rel_CDPCompletionData_empno into HDFS directory as a file 

STORE Rel_CDPCompletionData_empno INTO '/user/Pig_Output/Rel_CDPCompletionData_empno.csv' USING PigStorage(',');

--Split the relation Rel_CDPCompletionData_empno into 3 new relations based on txtPlanCategory 

SPLIT Rel_CDPCompletionData_empno INTO 
Rel_CDP_OnDemand IF txtPlanCategory == 'On Demand',
Rel_CDP_NeedBased IF txtPlanCategory == 'Need Based',
Rel_CDP_RoleBased IF txtPlanCategory == 'Role Based';

--Store the relation into Rel_CDP_OnDemand,Rel_CDP_NeedBased,Rel_CDP_RoleBased HDFS directory as a file 

STORE Rel_CDP_OnDemand INTO '/user/Pig_Output/Rel_CDP_OnDemand.csv' USING PigStorage(',');

STORE Rel_CDP_NeedBased INTO '/user/Pig_Output/Rel_CDP_NeedBased.csv' USING PigStorage(',');

STORE Rel_CDP_RoleBased INTO '/user/Pig_Output/Rel_CDP_RoleBased.csv' USING PigStorage(',');

--Create and load into a relation named Rel_CertificationDetails_empno using the Certification_empno file residing in the HDFS directory 

Rel_CertificationDetails_empno= LOAD '/user/Certification_empno/CertificationDetails.csv' USING PigStorage(',') AS 
(
CertificationTechnology:chararray,
CertificationCode:chararray,
CertificationTitle:chararray,
CompetencyDimension:chararray,
Certification_Type:chararray,
Complexity_Level:chararray
);

--Store the relation Rel_CertificationDetails_empno into  HDFS directory as a file 

STORE Rel_CertificationDetails_empno INTO '/user/Pig_Output/Rel_CertificationDetails_empno.csv' USING PigStorage(',');

--Joining two relations Rel_CDPCompletionData_empno and Rel_CertificationDetails_empno via a common key

employee_training_data = JOIN Rel_CDPCompletionData_empno BY CertificationCode,Rel_CertificationDetails_empno BY CertificationCode;

--Store the relation employee_training_data into  HDFS directory as a file 

STORE employee_training_data INTO '/user/Pig_Output/employee_training_data.csv' USING PigStorage(',');

--Filter the certifications that comes under external type and store it in an HDFS directory

external_certifications = FILTER Rel_CertificationDetails_empno BY Certification_Type == 'External';

--Store the relation external_certifications into  HDFS directory as a file

STORE external_certifications INTO '/user/Pig_Output/external_certifications.csv' USING PigStorage(',');

--Certification Insights:

--Identify unique certifications available for each role 

unique_certifications  =  FOREACH (GROUP Rel_CDPCompletionData_empno BY (RoleCapability)){ 
                          unique_certification_per_group = DISTINCT Rel_CDPCompletionData_empno.CertificationTitle;
                          GENERATE group,unique_certification_per_group;
                          }

--Storing the relation unique_certifications into  HDFS directory as a file

STORE unique_certifications INTO '/user/Pig_Output/unique_certifications.csv' USING PigStorage(',');

--How many employees under a specific role are skilled in a particular technology

specific_role_particular_tech  = FILTER employee_training_data BY Rel_CDPCompletionData_empno::RoleCapability== 'Technology Lead' AND Rel_CertificationDetails_empno::CertificationTechnology == 'BigData';

employee_count_specific_role_particular_tech= FOREACH (GROUP specific_role_particular_tech BY (RoleCapability,CertificationTechnology)){
                                              GENERATE group AS specifice_role_particular_tech,COUNT(specific_role_particular_tech);
                                               }

--Storing the relation employee_count_specific_role_particular_tech relation into  HDFS directory as a file
 
STORE employee_count_specific_role_particular_tech  INTO '/user/Pig_Output/employee_count_specific_role_particular_tech.csv' USING PigStorage(',');

--How many employees have cleared a specific certification

employees_cleared_specific_specification = FILTER employee_training_data BY Rel_CertificationDetails_empno::CertificationTitle=='Datastax - Certified Administrator on Apache Cassandra' AND Result == 'Qualified';

employee_count_specific_certification = FOREACH ( GROUP employees_cleared_specific_specification BY Rel_CertificationDetails_empno::CertificationTitle){
                                        GENERATE group AS certification,COUNT(employees_cleared_specific_specification) AS employee_count;
                                        }

--Storing the relation employee_count_specific_certification relation into  HDFS directory as a file

STORE employee_count_specific_certification INTO '/user/Pig_Output/employee_count_specific_certification.csv' USING PigStorage(',');

--How many employees are skilled in a specific technology

employees_cleared_specific_technology = FILTER employee_training_data BY Rel_CertificationDetails_empno::CertificationTechnology=='BigData' AND Rel_CDPCompletionData_empno::Result=='Qualified';
                       
employee_count_particular_technology = FOREACH(GROUP employees_cleared_specific_technology BY CertificationTechnology){
                                       GENERATE group AS technology,COUNT(employees_cleared_specific_technology) AS employee_count;
                                       }

--Storing the relation employee_count_particular_technology relation into  HDFS directory as a file

STORE employee_count_particular_technology INTO '/user/Pig_Output/employee_count_particular_technology.csv' USING PigStorage(',');

--How many employees have cleared a certification in first attempt

employees_cleared_certification = FILTER employee_training_data BY AttendedStatus == 'Attended' AND Result == 'Qualified';

employee_attempts_count= FOREACH (GROUP employees_cleared_certification BY (Rel_CDPCompletionData_empno::EmpNo,Rel_CertificationDetails_empno::CertificationTitle)){
                               GENERATE FLATTEN(group) AS  (EmpNo,CertificationTitle),(int)COUNT(employees_cleared_certification) AS attempt_count;
                               }

first_attempt= FILTER employee_attempts_count BY attempt_count == 1;

employee_count_cleared_first_attempt = FOREACH (GROUP first_attempt ALL) GENERATE COUNT(first_attempt.EmpNo) AS employee_count;

--Storing the relation employee_count_cleared_first_attempt relation into  HDFS directory as a file

STORE employee_count_cleared_first_attempt INTO '/user/Pig_Output/employee_count_cleared_first_attempt.csv' USING PigStorage(',');
