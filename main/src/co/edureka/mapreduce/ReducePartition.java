package co.edureka.mapreduce;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducePartition extends Reducer<Text,IntWritable,Text,IntWritable> 
{
	public void reduce(Text cluster, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException 
	{
		context.write(new Text(cluster),new IntWritable(1));
	}
}
