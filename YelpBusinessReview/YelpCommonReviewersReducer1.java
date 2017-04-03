package com.BigDataClass.Assin3;
import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

/**
 * This reducer for the Yelp Max Business Rating problem. This reducer receives
 * a key as pair of(Business_id and Business_Nmae). The reducer simply emits the
 * combination Business_id and Business_Nmae of both busineses and only count of
 * unique user
 * 
 * @author Rajesh Jaiswal
 *
 */
public class YelpCommonReviewersReducer1 extends Reducer<Text, Text, Text, Text> {
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		ArrayList<String> businessCommonReview = new ArrayList<String>();
		for (Text value : values) {
			businessCommonReview.add(value.toString());
		}

		for (int i = 0; i < businessCommonReview.size(); i++) {
			String strOne = businessCommonReview.get(i).toString();
			for (int j = i + 1; j < businessCommonReview.size(); j++) {
				String strTwo = businessCommonReview.get(j).toString();
				context.write(new Text(strOne + "\t" + strTwo), new Text(key));
			}
		}
	}
}
