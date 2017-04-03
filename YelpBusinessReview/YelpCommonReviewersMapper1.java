package com.BigDataClass.Assin3;
import java.io.IOException;
import java.io.*;
import java.util.HashMap;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * This Mapper for the Yelp Max Business Rating problem. This receives each line
 * from input data file as a join of business dataset and review file dataset.
 * The Mapper simply emits the combination of (Business_ID and Business_name
 * combination of both businesses and user_id)
 * 
 * @author Rajesh Jaiswal
 *
 */

public class YelpCommonReviewersMapper1 extends Mapper<LongWritable, Text, Text, Text> {
	HashMap<String, String> businessRatingDataSet = new HashMap<String, String>();

	// The setup method is executed per input split
	public void setup(Context context) throws IOException {
		Path pt = new Path("yelp_A.json");
		FileSystem fs = FileSystem.get(context.getConfiguration());

		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(pt)));
		String line = br.readLine();
		while (line != null) {
			try {
				JSONObject jsn = new JSONObject(line.toString());

				String business_id = (String) jsn.get("business_id");
				String business_name = (String) jsn.get("name");
				businessRatingDataSet.put(business_id, business_name);
				line = br.readLine();
			} catch (JSONException e) {
				e.printStackTrace();
			}

		}
	}

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			JSONObject jsn = new JSONObject(value.toString());
			String business_id = (String) jsn.get("business_id");
			String user_id = (String) jsn.get("user_id");

			if (businessRatingDataSet.containsKey(business_id)) {
				String business_name = businessRatingDataSet.get(business_id);
				context.write(new Text(user_id), new Text(business_id + "(" + business_name + ")"));
			}
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
}
