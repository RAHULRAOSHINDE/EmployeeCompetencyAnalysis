#!/bin/bash 

#Run this script to execute hadoop commands to managing directories and copying files in HDFS

#Modify the filenames and paths as needed

#Usage: ./Working_with_HDFS.sh

#specifying the paths to data

CertificationDetails="/root/Desktop/Emp_Competency/Data_Source/CertificationDetails.csv"

CDPCompletionStatus="/root/Desktop/Emp_Competency/Data_Source/CDPCompletionStatus.csv"

#creates a new directory Certification_empno in HDFS under the user directory
 
hadoop fs -mkdir /user/Certification_empno

#creates a new directory CDPCompletionStatus_empno in HDFS under the user directory 

hadoop fs -mkdir /user/CDPCompletionStatus_empno

#copies file from local file system to the Certification_empno directory in HDFS

hadoop fs -copyFromLocal "$CertificationDetails" /user/Certification_empno

#copies file from local file system to CDPCompletionStatus_empno directory in HDFS

hadoop fs -copyFromLocal "$CDPCompletionStatus" /user/CDPCompletionStatus_empno

