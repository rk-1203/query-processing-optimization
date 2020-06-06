package co.edureka.mapreduce;

import java.io.IOException;
import java.util.*;
//import java.util.StringTokenizer;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.google.gson.Gson;

public class MapReduceCluster extends Mapper<LongWritable,Text,DoubleWritable,Text> 
{
	Configuration conf;
    String inst;
    HashMap<String,Double> table_cluster;
    public void setup(Context context) 
    {
        conf = context.getConfiguration();
        inst = conf.get("table_cluster");
        Gson gson = new Gson();
        table_cluster = gson.fromJson(inst, HashMap.class);
        //System.out.println(hashset.getString1());
    } 
	public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException
	{
		String line = value.toString();
		String[] uri=line.split("\t");
		//System.out.println("uri[1] "+uri[1]);
		if(!table_cluster.containsKey(uri[1].trim()))
		{
			context.write(new DoubleWritable(100.0-Double.parseDouble(uri[0].trim())), new Text(uri[1].trim()));
		}
				
	}
}