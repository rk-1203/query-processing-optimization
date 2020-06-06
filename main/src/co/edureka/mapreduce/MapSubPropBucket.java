package co.edureka.mapreduce;

import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapSubPropBucket extends Mapper<LongWritable,Text,Text,Text> 
{
	public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException
	{
		String line = value.toString();
		String[] uri=line.split(" ");
		Text subject = new Text(uri[0]),predicate = new Text(uri[1]);
		context.write(subject,predicate);
	}
}
