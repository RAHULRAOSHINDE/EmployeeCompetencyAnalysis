package com.employeecompetencyanalysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class EmployeesQualifiedCertificationMapper extends Mapper<LongWritable,Text,LongWritable,Text> {
	//Text variable to hold the certificationTitle and attendedstatus
	private Text empInfo = new Text();
	//LongWritable variable to hold the empno  or Id
	private LongWritable empNo = new LongWritable();
	public void map(LongWritable key,Text values,Context context) throws IOException, InterruptedException {
		//splitting the input record into a array of strings by a , delimiter
		String records [] = values.toString().split(",");
		//setting output key
		empNo.set(Long.parseLong(records[0]));
		//setting the output value
		empInfo.set(records[4]+","+records[7]);
		context.write(empNo,empInfo);	
	}
}
