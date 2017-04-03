package com.BigDataClass.Assin3; 
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;

/**
 * A comparator for the the Key-Pair class. This comparator compares only by BusinessID and Business_Name for both business.
 * This get used in comparator is used for grouping the key pairs.
 * @author Rajesh Jaiswal
 *
 */

public class MapKeyPair implements WritableComparable<MapKeyPair> {

	// the key pair holds the businessID and stars
	private Text business_id_One;
	private Text business_id_Two;

	// The defaule constructor
	public MapKeyPair() {
		business_id_One = new Text();
		business_id_Two = new Text();
	}

	// constructor, initializing the businessID and stars
	public MapKeyPair(String bida, String bidb) {
		business_id_One = new Text(bida);
		business_id_Two = new Text(bidb);

	}

	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		business_id_One.readFields(in);
		business_id_Two.readFields(in);
	}

	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		business_id_One.write(out);
		business_id_Two.write(out);
	}

	public int compareTo(MapKeyPair otherPair) {
		// TODO Auto-generated method stub
		int c = business_id_One.compareTo(otherPair.business_id_One);
		if (c != 0)
			return c;
		else
			return business_id_Two.compareTo(otherPair.business_id_Two);
	}

	// the Getter and setter methods
	public Text getbusiness_id_One() {
		return business_id_One;
	}

	public void setbusiness_id_One(Text bida) {
		this.business_id_One = bida;
	}

	public Text getbusiness_id_Two() {
		return business_id_Two;
	}

	public void setbusiness_id_Two(Text bidb) {
		this.business_id_Two = bidb;
	}
}