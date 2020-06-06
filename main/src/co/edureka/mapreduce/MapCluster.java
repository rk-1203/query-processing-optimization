package co.edureka.mapreduce;

import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapCluster extends Mapper<LongWritable,Text,DoubleWritable,Text> 
{
	public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException
	{	
		Configuration conf = context.getConfiguration();
		double SUPPORT_THRESHOLD = Double.parseDouble(conf.get("SUPPORT_THRESHOLD"));
		int totalPropCluster = Integer.parseInt(conf.get("totalPropCluster"));	
		String line = value.toString();
		String[] tokens = line.split("\t"); // This is the delimiter between
		String keypart = (tokens[0]);
		int valuePart = Integer.parseInt(tokens[1]);
		double support = (valuePart*100.0)/totalPropCluster;
		System.out.println(keypart + " "+(keypart.split(" ")).length);
		if( support > SUPPORT_THRESHOLD && (keypart.split(" ")).length > 1 )
		{
			context.write(new DoubleWritable(100-support), new Text(keypart));
		}
	}
}