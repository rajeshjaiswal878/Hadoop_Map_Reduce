package uis.bigData.AverageFlightDelay;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.*;

/**
 * Driver class for the Average Delay for each Unique Carrier
 * We used combiner function for this programm to solve this mapreduce problem 
 * @author rajeshjaiswal
 *
 */

public class AverageDelayDriver extends Configured implements Tool {
		  public static void main(String[] args) throws Exception {	  
		    	int exitCode = ToolRunner.run(new Configuration(), new AverageDelayDriver(),args);
		    System.exit(exitCode);
		  }
		
		  public int run(String[] args) throws Exception {
			  if (args.length != 2) {
			      System.err.println("Usage: Mean elapsed time <input path> <output path>");
			      System.exit(-1);
			    }
			    //Initializing the map reduce job
			    @SuppressWarnings("deprecation")
				Job job= new Job();		
			    job.setJarByClass(AverageDelayDriver.class);
			    job.setJobName("Average Delay for Each Unique Carrier Flight");
			    job.setNumReduceTasks(6);

			    //Setting the input and output paths.The output file should not already exist. 
			    FileInputFormat.addInputPath(job, new Path(args[0]));
			    FileOutputFormat.setOutputPath(job, new Path(args[1]));

			    //Setting the mapper, reducer, and combiner classes
			    job.setMapperClass(AverageDelayMapper.class);
			    job.setReducerClass(AverageDelayReducer.class);
			    job.setCombinerClass(AverageDelayCombiner.class);
			    
			    //Setting output file format
			    job.setMapOutputKeyClass(Text.class);
			    job.setMapOutputValueClass(AverageDelayPair.class);
			    
			    //Setting the format of the key-value pair to write in the output file.
			    job.setOutputKeyClass(Text.class);
			    job.setOutputValueClass(DoubleWritable.class);

			    //Submit the job and wait for its completion
			    return(job.waitForCompletion(true) ? 0 : 1);
			}
	  
    
  }
