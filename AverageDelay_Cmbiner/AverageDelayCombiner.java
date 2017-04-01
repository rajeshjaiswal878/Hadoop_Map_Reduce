package uis.bigData.AverageFlightDelay;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
/**
 * Combiner Class for the Average Delay Flight problem with 6 Number of Reducer
 * @author Rajesh Jaiswal
 *
 */

public class AverageDelayCombiner extends
Reducer<Text,AverageDelayPair, Text, AverageDelayPair> {
public void reduce(Text key, Iterable<AverageDelayPair> values, Context context)
  throws IOException, InterruptedException {
double sum = 0;
ArrayList<Double> departureDelays = new ArrayList<Double>();
for (AverageDelayPair value : values) {
  sum += value.getPartialSum();
  departureDelays.add(value.getPartialSum());
}
context.write(key, new AverageDelayPair(sum,departureDelays.size()));
} 

}
