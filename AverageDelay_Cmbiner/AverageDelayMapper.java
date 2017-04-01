package uis.bigData.AverageFlightDelay;
import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
/**
 * Mapper class for the Average Delay for each Unique Carrier
 * We used mapper function for this programm will mapp key value according to uniqueCarrier problem 
 * @author rajeshjaiswal
 *
 */
public class AverageDelayMapper extends
    Mapper<LongWritable, Text, Text, AverageDelayPair> {
  public void map(LongWritable key,Text value, Context context)
      throws IOException, InterruptedException {
   String[] columns  = value.toString().split(",");
   
   String uniqueCarrier="";String departureDealy="";
   if(columns[15].equals("DepDelay"))
   {
	   return;
   }
   else
   {
  try
   { 
	   if(!columns[8].equalsIgnoreCase("NA")&&!columns[15].equalsIgnoreCase("NA"))
		  {
			  uniqueCarrier = columns[8];
			  departureDealy=columns[15];
	   context.write(new Text(uniqueCarrier),new AverageDelayPair(Double.parseDouble(departureDealy),1));
   }
   }
   catch(Exception e)
   {
	   e.printStackTrace();
   }
    }
  }
}

