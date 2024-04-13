package com.employeecompetencyanalysis;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/*Reducer class for aggregrating the total attempts for a specific certification
Input - (CertificationTitle,[1,1,1,..])
Output -(CertificationTitle, TotalAttempts)*/

public class SpecificCertificationAttemptsReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
	public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		int totalAttempts =0;
		//Summing up the attempt counts for a specific certification
		for(IntWritable value:values) {
			totalAttempts= totalAttempts+value.get();
		}
		//Emitting the result as CertificationTitle and totalattempts count
		context.write(key,new IntWritable(totalAttempts));
	}
}
