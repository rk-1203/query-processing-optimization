package co.edureka.mapreduce;

import java.io.IOException;
//import java.util.StringTokenizer;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.google.gson.Gson;

public class MapMakeTable extends Mapper<LongWritable,Text,Text,Text> 
{
	String prop;
	Configuration conf;
    
    public void setup(Context context) 
    {
        conf = context.getConfiguration();
        prop = conf.get("prop");
        Gson gson = new Gson();
        prop = gson.fromJson(prop, String.class).trim();
    } 
    
	public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException
	{
		String[] triple = value.toString().split(" ");
		if(prop.equals(triple[1].trim()))
			context.write(new Text(triple[0].trim()), new Text(triple[2].trim()));		
	}
}