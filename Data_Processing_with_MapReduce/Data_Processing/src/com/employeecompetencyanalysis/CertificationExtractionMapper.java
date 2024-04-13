package com.employeecompetencyanalysis;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CertificationExtractionMapper extends Mapper<LongWritable,Text,LongWritable,Text>{
	//Intitalizing text objects for Output
	private Text fieldsExtracted = new Text();
	//Intitalizing the LongWritable object for Output
	private LongWritable empNo = new LongWritable();
	public void map(LongWritable key,Text values,Context context) throws IOException, InterruptedException {
		String [] records = values.toString().split(",");
		//Setting the output  value
		fieldsExtracted.set(records[1]+","+records[2]+","+records[4]+","+records[5]+","+records[14]+","+records[11]+","+records[12]);
		//Setting the output key
		empNo.set(Long.parseLong(records[0]));
		context.write(empNo,fieldsExtracted);
	}
}

/*
Output --> EmpNo,RoleCapbility,EmpPUCode,CertificationCode,CertificationTitle,Result,Onsite_offshore,AttendedStatus
 */