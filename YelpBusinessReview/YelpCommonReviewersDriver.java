package com.BigDataClass.Assin3;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * This is the Driver class containing step by step execution of 2 Mappers and 2 reducer to solve Yelp Data set  problem.
 * This receives each line from input data file as a key as pair of(Business_ID and Business_Name in Mapper 1) and COmbination 2 entry combination as in Mapper 2 .
 * The Mapper Fnally Reducer emit answer in[ Business_id1(Business_Name)  Business_id2(Business_Name) CountOfUniqueReviewrs ]
 *
 * @author Rajesh Jaiswal
 *
 */

public class YelpCommonReviewersDriver extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
	
		int exitCode = ToolRunner.run(new Configuration(),new YelpCommonReviewersDriver(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
		      System.err.println("Usage: Yelp Join <input path> <output path>");
		      System.exit(-1);
		    }
		    //Initializing the map reduce job
		    @SuppressWarnings("deprecation")
			Job job1= new Job(getConf());
		    job1.setJarByClass(YelpCommonReviewersDriver.class);
		    job1.setJobName("Mapper One Steps to read Yelp File");
		    //job1.setNumReduceTasks(6);

		    //Setting the input and output paths.The output file should not already exist. 
		    FileInputFormat.addInputPath(job1, new Path(args[0]));
		    Path tempOut=new Path("Temp");
		    SequenceFileOutputFormat.setOutputPath(job1, tempOut);
			
		    //Setting the mapper, reducer, and combiner classes
		    job1.setMapperClass(YelpCommonReviewersMapper1.class);
		    job1.setReducerClass(YelpCommonReviewersReducer1.class);
		    job1.setMapOutputKeyClass(Text.class);
		    job1.setMapOutputValueClass(Text.class);
		    job1.setOutputKeyClass(Text.class);
		    job1.setOutputValueClass(Text.class);
		    job1.waitForCompletion(true);

		    //-----------------------------------------------------------------


		  //Job2 Step By Step Processing
		    @SuppressWarnings("deprecation")
			Job job2= new Job(getConf());
		    job2.setJarByClass(YelpCommonReviewersDriver.class);
		    job2.setJobName("Mapper One Steps to read Yelp File");
		    //job2.setNumReduceTasks(6);

		    //Setting the input and output paths.The output file should not already exist. 
		    SequenceFileInputFormat.addInputPath(job2, tempOut);
		    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
			
		    //Setting the mapper, reducer, and combiner classes
		    job2.setMapperClass(YelpCommonReviewersMapper2.class);
		    job2.setReducerClass(YelpCommonReviewersReducer2.class);
		    job2.setMapOutputKeyClass(MapKeyPair.class);
		    job2.setMapOutputValueClass(Text.class);
		    job2.setOutputKeyClass(Text.class);
		    job2.setOutputValueClass(IntWritable.class);
		    
		    // Setting the grouping and sorting comparator classes
			job2.setCombinerKeyGroupingComparatorClass(YelpCommonReviewersIdComparator.class);
		    job2.waitForCompletion(true);
		   
		    return(job2.waitForCompletion(true) ? 0 : 1);
		  }

	}
