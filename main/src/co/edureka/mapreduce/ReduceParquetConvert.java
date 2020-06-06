package co.edureka.mapreduce;


import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;

import com.google.gson.Gson;

public  class ReduceParquetConvert extends  Reducer<Text,LongWritable,Text,LongWritable>  
{
	
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
	
	public void reduce(Text key, Iterable<LongWritable> values, Context output)throws IOException, InterruptedException
	{
		
	}
}