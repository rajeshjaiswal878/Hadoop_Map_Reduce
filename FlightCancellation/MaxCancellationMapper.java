package uis.bigdataClass.MaxCancellation;
import java.io.IOException;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class MaxCancellationMapper extends
    Mapper<LongWritable, Text, Text, Text> {
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  String[] columns  = value.toString().split(",");
	  String uniqueCarrier=null,source=null,destination=null;
	  String s=columns[21];
	  if(s.equals("Cancelled"))
	  {
		  return;
	  }	 
	  else{
	  int i=Integer.valueOf(s);
	  if(i==1)
	  {	  
	  if((!columns[8].equalsIgnoreCase("NA"))&&(!columns[16].equalsIgnoreCase("NA"))&&(!columns[17].equalsIgnoreCase("NA")))
	  {
		  uniqueCarrier = columns[8];
		  source = columns[16];
		  destination = columns[17];
	  }
	  /*if(!columns[16].equalsIgnoreCase("NA"))
	  {
		  source = columns[16];
	  }
	  if(!columns[17].equalsIgnoreCase("NA"))
	  {
		  destination = columns[17];
	  }*/
	   
	   context.write(new Text(uniqueCarrier+" | "),new Text(source+" | "+destination+" | "));
	  }  
	  }
  }
}


