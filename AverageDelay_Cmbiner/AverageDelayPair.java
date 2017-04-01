package uis.bigData.AverageFlightDelay;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.*;

/**
 * Pair class for the Average Delay for each Unique Carrier
 * We used Partial Summation function for this programm will mapp key value according to uniqueCarrier problem 
 * @author rajeshjaiswal
 *
 */
public class AverageDelayPair implements Writable{

private DoubleWritable partialSum;
private IntWritable partialCount;

//Default constructor
public AverageDelayPair()
{
	partialSum = new DoubleWritable(0);
	partialCount=new IntWritable(0);
}

//Constructor, initializing the partial sum and partial count
public AverageDelayPair(double sum, int count)
{
	this.partialSum= new DoubleWritable(sum);
	this.partialCount = new IntWritable(count);
}

//Accessors and Mutators
public double getPartialSum()
{
	return this.partialSum.get();
}

public int getPartialCount()
{
	return this.partialCount.get();
}

public void setPartialSum(double sum)
{
	this.partialSum=new DoubleWritable(sum);
}

public void setPartialCount(int count)
{
	this.partialCount= new IntWritable(count);
}


public void readFields(DataInput in) throws IOException {
	partialSum.readFields(in);
	partialCount.readFields(in);
	
}
public void write(DataOutput out) throws IOException {
	// TODO Auto-generated method stub
	partialSum.write(out);
	partialCount.write(out);
}
}