package uis.bigdataClass.MaxCancellation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class MaxCancellationReducer extends
    Reducer<Text, Text, Text, Text> {
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
	  ArrayList<String> lstDestinations = new ArrayList<String>();
	  for (Text value : values) {	      
		  lstDestinations.add(value.toString());	      
	   }
	  ArrayList<String> newList = new ArrayList<String>(new LinkedHashSet<String>(lstDestinations));
	  int frequents[] = new int[newList.size()];
	  int n=0;
	  for(String item : newList)
	  {
		  frequents[n] = Collections.frequency(lstDestinations, item);
		  n++;
	  }
	  int mostfrequent = frequents[0];
	  String mostfrequentdest = newList.get(0);
	  for(int i=1;i<frequents.length;i++)
	  {
		  if(mostfrequent<frequents[i])
		  {
			  mostfrequent = frequents[i];
			  mostfrequentdest = newList.get(i);
		  }
	  }
	  context.write(key,new Text(mostfrequentdest+""+Integer.toString(mostfrequent)+"|"));
	  
  }
}
