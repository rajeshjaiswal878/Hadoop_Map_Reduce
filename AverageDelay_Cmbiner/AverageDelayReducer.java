package uis.bigData.AverageFlightDelay;
import java.io.IOException;
/**
 * Reducer class for the Average Delay for each Unique Carrier
 * We used Partial Summation function for this programm will mapp key value according to uniqueCarrier problem 
 * @author rajeshjaiswal
 *
 */
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
public class AverageDelayReducer extends
Reducer<Text,AverageDelayPair, Text, DoubleWritable> {
public void reduce(Text key, Iterable<AverageDelayPair> values, Context context)
  throws IOException, InterruptedException {
double sumofALL = 0;
int countALL=0;
//Adding the partial sums and partial counts
for (AverageDelayPair value : values) {
  sumofALL += value.getPartialSum();
  countALL+= value.getPartialCount();
}
context.write(key, new DoubleWritable(sumofALL/countALL));
} 

}