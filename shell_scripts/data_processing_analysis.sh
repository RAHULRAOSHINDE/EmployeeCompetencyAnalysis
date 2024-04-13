#!/bin/bash

#Run hadoop tasks

echo "Running hadoop tasks"

./Working_With_HDFS.sh

#Specifying paths to data and scripts

MapReduce_Jar="/root/Desktop/Emp_Competency/Data_Processing_with_MapReduce/CertificationAnalysis.jar"

CertificationDetails="/user/Certification_empno/CertificationDetails.csv"

CDPCompletionStatus="/user/CDPCompletionStatus_empno/CDPCompletionStatus.csv"

Hive_Script="/root/Desktop/Emp_Competency/Data_Analysis_using_Hive/Data_Analysis_using_Hive.hql"

Pig_Script="/root/Desktop/Emp_Competency/Data_Analysis_using_Pig/Data_Analysis_using_Pig.pig"

#Run Hadoop commands 

#MapReduce Job

echo "Running mapreduce jobs"

hadoop jar "$MapReduce_Jar" 

echo "MapReduce execution completed"  


#Run Pig Script

echo "Running Pig Script"

pig -x mapreduce "$Pig_Script"

#Exit from pig shell

echo "Pig script execution completed"

quit;

#Run Hive queries

echo "Running hive queries"

hive -f "$Hive_Script";

#Exit from hive shell  

quit;

echo "Hive queries execution completed"

