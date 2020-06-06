package co.edureka.mapreduce;


import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;


public class ReduceReduceCluster extends Reducer<DoubleWritable,Text,DoubleWritable,Text> 
{
	//static int totalPropCluster = 0;
	/*public void reduce(Text propCluster, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException 
	{
		

		double sum=0;
		for(IntWritable x: values)
		{
			sum+=x.get();
		}
		context.write(new DoubleWritable(sum), propCluster);
	}*/
	
	public void reduce(DoubleWritable supPer, Iterable<Text> propList, Context context) throws IOException,InterruptedException 
	{
		for (Text value : propList)
			context.write(new DoubleWritable(100.0-supPer.get()), value);
	}
}