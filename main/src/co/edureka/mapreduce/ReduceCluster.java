package co.edureka.mapreduce;


import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReduceCluster extends Reducer<DoubleWritable,Text,DoubleWritable,Text> 
{
	public void reduce(DoubleWritable support, Iterable<Text> list,Context context) throws IOException,InterruptedException 
	{
		for (Text value : list) 
			context.write(new DoubleWritable(100-support.get()), value);
	}
}