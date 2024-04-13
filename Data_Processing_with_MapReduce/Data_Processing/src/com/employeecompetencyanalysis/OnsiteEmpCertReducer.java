package com.employeecompetencyanalysis;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/*Reducer class for aggregrating the  number of attemtps for certification */
public class OnsiteEmpCertReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
	//IntWritable variable to hold occurrence count
	private IntWritable  onsiteEmpAttempts = new IntWritable();
	public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
		int certAttempts = 0;
		//aggregrating all the occurrences count,summing up and accumulating in CertAttempts  
		for(IntWritable value:values) {
			certAttempts = certAttempts+value.get();
		}
		//setting the output value 
		onsiteEmpAttempts.set(certAttempts);
		//emitting the results 
		context.write(key,onsiteEmpAttempts);
}
}
