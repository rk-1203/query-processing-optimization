package co.edureka.mapreduce;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;

public class MapClusterSetCount extends Mapper<LongWritable,Text,Text,LongWritable> 
{
	Configuration conf;
    String inst;
    HashMap<String,Double> propMap;
    
    public void setup(Context context) 
    {
        conf = context.getConfiguration();
        inst = conf.get("propMap");
        Gson gson = new Gson();
        propMap = gson.fromJson(inst, HashMap.class);
    } 
	
	public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException
	{	
		Configuration conf = context.getConfiguration();
		String line = value.toString();
		String[] uri=line.split("\t");
		long count = 0;
        HashSet<String> set1 = new HashSet<String>(Arrays.asList(uri[0].trim().split(" ")));
        for (HashMap.Entry<String,Double> x : propMap.entrySet())
        {
        	System.out.println("???????????????????????????????????????????????/Key = " + x.getKey() + ", Value = " + x.getValue());
        	String s = x.getKey();
        	HashSet<String> set2 = new HashSet<String>(Arrays.asList(s.trim().split(" ")));
        	if(set2.containsAll(set1))
        	{
        		double v = x.getValue();
        		count = count + (long)v;
        	}
        }
		context.write(new Text(uri[0]), new LongWritable(count));	
	}
}