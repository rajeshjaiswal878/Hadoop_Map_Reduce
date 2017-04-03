package com.BigDataClass.Assin3;
import org.apache.hadoop.io.*;

/**
 * A comparator for the the Key-Pair class. This comparator compares only by
 * BusinessID and Business_Name. This comparator is used for grouping the key
 * pairs.
 * 
 * @author Rajesh Jaiswal
 *
 */
public class YelpCommonReviewersIdComparator extends WritableComparator {

	public YelpCommonReviewersIdComparator() {
		super(MapKeyPair.class, true);

	}

	public int compare(WritableComparable k1, WritableComparable k2) {
		MapKeyPair key1 = (MapKeyPair) k1;
		MapKeyPair key2 = (MapKeyPair) k2;
		return key1.getbusiness_id_One().compareTo(key2.getbusiness_id_Two());
	}
}