package co.edureka.mapreduce;


import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducePropCluster extends Reducer<Text,IntWritable,Text,IntWritable> 
{
	//static int totalPropCluster = 0;
	public void reduce(Text propCluster, Iterable<IntWritable> values,Context context) throws IOException,InterruptedException 
	{

		int sum=0;
		for(IntWritable x: values)
		{
			sum+=x.get();
		}
		context.write(propCluster, new IntWritable(sum));
	}
}