package com.employeecompetencyanalysis;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CertificationAnalysisDriver {
	public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
		//This configuration setup is to extract the fields from the CDPCompletionStatus
		Configuration extractionConf = new Configuration();
		// Set the output separator to ","
		extractionConf.set("mapreduce.output.textoutputformat.separator", ",");
		Job extractionJob  = Job.getInstance(extractionConf);
		extractionJob.setJobName("FieldsExtraction");
		extractionJob.setJarByClass(CertificationAnalysisDriver.class);
		extractionJob.setMapperClass(CertificationExtractionMapper.class);
		extractionJob.setOutputKeyClass(LongWritable.class);
		extractionJob.setOutputValueClass(Text.class);
		extractionJob.setOutputFormatClass(TextOutputFormat.class);
		Path InputPath = new Path("/user/CDPCompletionStatus_empno/CDPCompletionStatus.csv");
		Path OutputPath = new Path("/user/mapreduce/FieldsExtraction");
		FileInputFormat.addInputPath(extractionJob,InputPath);
		FileOutputFormat.setOutputPath(extractionJob,OutputPath);
		extractionJob.waitForCompletion(true);

		//This configuration setup is find the total number of attempts for specification
		Configuration specificCertConf= new Configuration();
		//Set the output separator to ","
		specificCertConf.set("mapreduce.output.textoutputformat.separator",",");
		Job specificCertJob = Job.getInstance(specificCertConf);
		specificCertJob.setJobName("SpecificCertifications");
		specificCertJob.setJarByClass(CertificationAnalysisDriver.class);
		specificCertJob.setMapperClass(SpecificCertificationAttemptsMapper.class);
		specificCertJob.setReducerClass(SpecificCertificationAttemptsReducer.class);
		specificCertJob.setOutputKeyClass(Text.class);
		specificCertJob.setOutputValueClass(IntWritable.class);
		specificCertJob.setOutputFormatClass(TextOutputFormat.class);
		Path inputFilePath= new Path("/user/mapreduce/FieldsExtraction/part-r-00000");
		Path outputFilePath = new Path("/user/mapreduce/SpecificCertifications");
		FileInputFormat.addInputPath(specificCertJob, inputFilePath);
		FileOutputFormat.setOutputPath(specificCertJob, outputFilePath);
		specificCertJob.waitForCompletion(true);

		//This configuration setup is to find the total number of employees qualified  for each certification 

		Configuration qualifiedEmpConf = new Configuration();
		Job qualifiedEmpJob = Job.getInstance(qualifiedEmpConf);
		//Set the output separator to ","
		qualifiedEmpConf.set("mapreduce.output.textoutputformat.separator",",");
		qualifiedEmpJob.setJobName("QualifiedEmployees");
		qualifiedEmpJob.setJarByClass(CertificationAnalysisDriver.class);
		qualifiedEmpJob.setMapperClass(EmployeesQualifiedCertificationMapper.class);
		qualifiedEmpJob.setReducerClass(EmployeesQualifiedCertificationReducer.class);
		qualifiedEmpJob.setMapOutputKeyClass(LongWritable.class);
		qualifiedEmpJob.setMapOutputValueClass(Text.class);
		qualifiedEmpJob.setOutputKeyClass(Text.class);
		qualifiedEmpJob.setOutputValueClass(IntWritable.class);
		qualifiedEmpJob.setOutputFormatClass(TextOutputFormat.class);
		Path inFilePath = new Path("/user/mapreduce/FieldsExtraction/part-r-00000");
		Path outFilePath = new Path("/user/mapreduce/QualifiedEmp");
		FileInputFormat.addInputPath(qualifiedEmpJob, inFilePath);
		FileOutputFormat.setOutputPath(qualifiedEmpJob, outFilePath);
		qualifiedEmpJob.waitForCompletion(true);
		
		//This configuration is to find the onsite employees who have attempted the certification
		
		Configuration onsiteEmpCertConf = new Configuration();
		onsiteEmpCertConf.set("mapreduce.output.textformatoutput.separatot",",");
		Job onsiteEmpCertJob = Job.getInstance(onsiteEmpCertConf);
		onsiteEmpCertJob.setJobName("OnsiteEmployeesAttempts");
		onsiteEmpCertJob.setJarByClass(CertificationAnalysisDriver.class);
		onsiteEmpCertJob.setMapperClass(OnsiteEmpCertMapper.class);
		onsiteEmpCertJob.setReducerClass(OnsiteEmpCertReducer.class);
		onsiteEmpCertJob.setOutputKeyClass(Text.class);
		onsiteEmpCertJob.setOutputValueClass(IntWritable.class);
		onsiteEmpCertJob.setOutputFormatClass(TextOutputFormat.class);
		Path inPath = new Path("/user/mapreduce/FieldsExtraction/part-r-00000");
		Path outPath = new Path("/user/mapreduce/OnsiteEmp");
		FileInputFormat.addInputPath(onsiteEmpCertJob, inPath);
		FileOutputFormat.setOutputPath(onsiteEmpCertJob,outPath);
		System.exit(onsiteEmpCertJob.waitForCompletion(true) ? 0 : 1);
	}
}