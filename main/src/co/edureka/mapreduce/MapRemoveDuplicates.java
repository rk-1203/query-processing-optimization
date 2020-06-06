package co.edureka.mapreduce;

import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapRemoveDuplicates extends Mapper<LongWritable,Text,Text,IntWritable> 
{
	public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException
	{
		String line = value.toString();
		String[] uri=line.split("\t");
		Text propCluster = new Text(uri[0]);
		context.write(propCluster, new IntWritable(1));		
	}
}