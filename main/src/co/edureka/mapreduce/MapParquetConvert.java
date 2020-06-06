package co.edureka.mapreduce;

import java.io.IOException;
//import java.util.StringTokenizer;

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
public class MapParquetConvert extends Mapper<LongWritable, Text, Void, GenericRecord> {
	
	Configuration conf;
    String inst,propList;
    Schema MAPPING_SCHEMA;
    
    public void setup(Context context) 
    {
        conf = context.getConfiguration();
        inst = conf.get("propList");
        Gson gson = new Gson();
        propList = gson.fromJson(inst, String.class);
       
        inst = conf.get("MAPPING_SCHEMA");
        gson = new Gson();
        MAPPING_SCHEMA = gson.fromJson(inst, Schema.class);
    } 
    
    private GenericRecord record = new GenericData.Record(MAPPING_SCHEMA);
    
     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
     {
         record.put("line", value.toString());
         context.write(null, record); 
     }        
}