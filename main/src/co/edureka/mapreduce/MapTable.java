package co.edureka.mapreduce;

import java.io.IOException;
//import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;

public class MapTable extends Mapper<LongWritable,Text,Text,IntWritable> 
{
	Configuration conf;
    String inst;
    HashSet<String> hashset;
    public void setup(Context context) 
    {
        conf = context.getConfiguration();
        inst = conf.get("hashset");
        Gson gson = new Gson();
        hashset = gson.fromJson(inst, HashSet.class);
        //System.out.println(hashset.getString1());
    } 
    
	public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException
	{		
		String line = value.toString();
		String[] uri=line.split("\t");
		String[] prop = uri[0].trim().split(" ");
		for(String x : prop)
		{
			if(hashset.contains(x)==false)
			{
				context.write(new Text(x),new IntWritable(1));				
			}
		}
	}

}
