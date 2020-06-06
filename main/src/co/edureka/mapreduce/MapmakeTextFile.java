package co.edureka.mapreduce;

import java.io.IOException;
//import java.util.StringTokenizer;
import java.util.HashSet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.google.gson.Gson;

// Map function
public class MapmakeTextFile extends Mapper<LongWritable, Text, Text, Text> 
{
     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
     {
         //record.put("line", value.toString());
    	 String[] temp= value.toString().split(" ");
         context.write(new Text(temp[0].trim()), new Text( temp[1].trim() +"_"+ temp[2].trim() )); 
     }        
}