package com.employeecompetencyanalysis;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/*Mapper class to filter and emit occurrences for  each certification
Input - (EmpNo)
Output- (certificationTitle,[1,1,1,..])*/

public class SpecificCertificationAttemptsMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	//Text variable to hold the certificationTitle
	private Text certificationTitle = new Text();
	//IntWritable variable to hold the count occurrences
	private IntWritable one = new IntWritable(1);
	public void map(LongWritable key,Text values,Context context) throws IOException, InterruptedException {
		//splitting the input record into array of strings by a  , delimiter
		String [] records = values.toString().split(",");
		//checking if the record has the specific certification and has an "Attended"
		if(records[4].equals("Certified Big Data  NoSQL MongoDB Developer") && records[7].equals("Attended")){
			//emitting the result as certificationTitle and occurrences
			certificationTitle.set(records[4]);
			context.write(certificationTitle,one);
		}		
	}

}
