package com.BigDataClass.Assin3;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
/**
 * This Mapper for the Yelp Max Business Rating problem. This receives each line
 * from input data file as a key as pair of(Business_ID and Business_name
 * combination of both businesses). The Mapper simply emits the combination of
 * (Business_ID and Business_name combination of both businesses and user_id)
 * 
 * @author Rajesh Jaiswal
 *
 */
public class YelpCommonReviewersMapper2 extends Mapper<LongWritable, Text, MapKeyPair, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			String[] strLine = value.toString().split("\t");
			String businessOne = strLine[0];
			String businessTwo = strLine[1];
			String uid = strLine[2];
			// this condition will eliminate same business on both side
			// condition
			if (businessOne.compareTo(businessTwo) != 0) {
				context.write(new MapKeyPair(businessOne, businessTwo),
						new Text(businessOne + "\t" + businessTwo + "\t" + uid));
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}