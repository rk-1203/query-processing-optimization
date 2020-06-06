package co.edureka.mapreduce;


import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;


public class ReduceClusterSetCount extends Reducer<Text,LongWritable,Text,LongWritable> 
{
	public void reduce(Text propCluster, Iterable<LongWritable> values,Context context) throws IOException,InterruptedException 
	{
		long sum=0;
		for(LongWritable x: values)
		{
			sum+=x.get();
		}
		context.write(propCluster, new LongWritable(sum));
	}
}