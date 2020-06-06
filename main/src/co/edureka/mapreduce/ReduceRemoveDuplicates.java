package co.edureka.mapreduce;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReduceRemoveDuplicates extends Reducer<Text,IntWritable,Text,IntWritable> 
{
	public void reduce(Text propCluster, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException 
	{
		context.write(propCluster, new IntWritable(1));
	}
}