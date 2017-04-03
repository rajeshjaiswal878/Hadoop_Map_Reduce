package com.BigDataClass.Assin3;
import java.io.IOException;
import java.util.LinkedHashSet;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**
 * This reducer for the Yelp Max Business Rating problem. This reducer receives
 * a key as pair of(Business_ID and Stars). The reducer simply emits the
 * combination of User_ID and only Max Stars Value
 * 
 * @author Rajesh Jaiswal
 *
 */
public class YelpCommonReviewersReducer2 extends Reducer<MapKeyPair, Text, Text, IntWritable> {
	public void reduce(MapKeyPair key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		LinkedHashSet<String> Final = new LinkedHashSet<String>();
		for (Text value : values) {
			Final.add(value.toString());
		}
		context.write(new Text(key.getbusiness_id_One().toString() + "\t" + key.getbusiness_id_Two().toString()),
				new IntWritable(Final.size()));
	}
}