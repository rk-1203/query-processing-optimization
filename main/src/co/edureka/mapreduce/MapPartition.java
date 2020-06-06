package co.edureka.mapreduce;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.gson.Gson;

public class MapPartition extends Mapper<LongWritable,Text,Text,IntWritable> 
{
	Configuration conf;
    String inst;
    HashMap<String,Double> propUsage,tempMap;
    
    public void setup(Context context) 
    {
        conf = context.getConfiguration();
        inst = conf.get("propUsage");
        Gson gson = new Gson();
        propUsage = gson.fromJson(inst, HashMap.class);
       
        inst = conf.get("tempMap");
        gson = new Gson();
        tempMap = gson.fromJson(inst, HashMap.class);
    } 
    
    public double nullPer(String propList)
    {
    	String[] prop = propList.split(" ");
    	long max=0;
    	double per = 0.0;
    	for( String x : prop )
    	{
    		double t = (double)propUsage.get(x);
    		long v = (long) t;
    		if(propUsage.containsKey(x) && v>max) 
    		{
    			System.out.println("########################################"+propUsage.get(x) );
    			// v = propUsage.get(x).longValue();
    			max = v;
    		}
    	}
    	for( String x : prop )
    	{
    		per+=(double)(max-propUsage.get(x).longValue());
    	}
    	per=per/((prop.length+1)*max);
    	return per;
    	//return 5;	
    }
        
	public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException
	{		
		Configuration conf = context.getConfiguration();
		double NULL_THRESHOLD = Double.parseDouble(conf.get("NULL_THRESHOLD"));
		String line = value.toString();
		String[] uri=line.split("\t");

		while( nullPer(uri[1].trim()) > NULL_THRESHOLD )
		{
			String prop[] = uri[1].trim().split(" ");
			String minProp=null;
			for(String x : prop)
			{
				if(minProp==null || propUsage.get(x)<propUsage.get(minProp))
					minProp = x;
			}
			uri[1] = uri[1].replace(" "+minProp+" ", " ");
			/*
			String prop[] = uri[1].trim().split(" ");
			for(String x: prop)
			{
				//double t = (double)tempMap.get(x);
	    		//long v = (long) t;
				if(tempMap.containsKey(x) && tempMap.get(x)>1.0)
				{
					ok=false;
					break;
				}
			}*/
		}
	}
}
