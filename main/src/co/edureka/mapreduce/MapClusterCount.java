package co.edureka.mapreduce;

import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapClusterCount extends Mapper<LongWritable,Text,Text,LongWritable> 
{
	public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException
	{	
		String line = value.toString();
		String v = line.split("\t")[1].trim();
		
		
	
		context.write(new Text("Total Lines"), new LongWritable(Long.parseLong(v)));
	}
}