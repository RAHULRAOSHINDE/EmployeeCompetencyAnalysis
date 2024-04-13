package com.employeecompetencyanalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*Mapper class to filter the records of employees who are on  onsite and have attended the certification exam 
Input -  EmpNo,RoleCapbility,EmpPUCode,CertificationCode,CertificationTitle,Result,Onsite_offshore,AttendedStatus
Output - EmpNo, [1,1,1,...]*/
public class OnsiteEmpCertMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	//Longwritable variable to hold the empNo 
	private Text location = new Text();
	//IntWritable variable to hold the occurrences
	private IntWritable one = new IntWritable(1);
	
	public void map(LongWritable key,Text values,Context context) throws IOException, InterruptedException {
		String [] records = values.toString().split(",");
		if(records[6].equals("Onsite") && records[7].equals("Attended")) {
			location.set(records[6]);
			context.write(location,one);
		}
	}

}
