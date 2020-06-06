package co.edureka.mapreduce;


import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReduceMakeTable extends Reducer<Text,Text,Text,Text> 
{
	public void reduce(Text subject, Iterable<Text> predicateList,Context context) throws IOException,InterruptedException 
	{
		for (Text value : predicateList) 
			context.write(subject, value);
	}
}