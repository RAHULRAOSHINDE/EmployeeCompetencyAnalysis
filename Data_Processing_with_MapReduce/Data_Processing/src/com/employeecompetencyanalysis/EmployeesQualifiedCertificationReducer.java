package com.employeecompetencyanalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class EmployeesQualifiedCertificationReducer extends Reducer<LongWritable,Text,Text,IntWritable> {
	//Text variable to hold the certificationTitle
	private Text certificationTitle = new Text();
	//IntWritable variable to hold the count of employees who have qualified the certification
	private IntWritable  totalQualifiedEmp = new IntWritable();
	public void reduce(LongWritable key,Iterable<Text> values,Context context) throws IOException, InterruptedException {
		int qualifiedEmpCount= 0;
		for(Text value:values) {
			//splitting the input into a array of strings by a , delimiter 
			String [] records = value.toString().split(",");
			qualifiedEmpCount+=1;
			//setting the output key as certificationTitle
			certificationTitle.set(records[0]);
			//setting the output value as totalQualifiedEmp
			totalQualifiedEmp.set(qualifiedEmpCount);
			//emitting the result as certificationTitle,qualifiedEmpCount
			context.write(certificationTitle, totalQualifiedEmp);
		}
	}
}
