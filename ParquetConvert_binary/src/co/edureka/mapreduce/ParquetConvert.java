package co.edureka.mapreduce;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.example.data.Group;


public class ParquetConvert extends Configured implements Tool
{
	static Schema MAPPING_SCHEMA ;
	static HashSet <String> propSet;
	
	public static class ParquetConvertMapper extends Mapper<LongWritable, Text,Void, GenericRecord> {
		
	    private GenericRecord record = new GenericData.Record(MAPPING_SCHEMA);

	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
	    {

	    	String triple[] = value.toString().trim().split("\t");
	    	String propList[] = triple[1].trim().split(" ");
	    	int flag = 0;
	    	HashMap<String, String> map = new HashMap();
	    	for(String x : propList)
	    	{
	    		String list[] = x.split("_");
	    		//System.out.println("propSet ***********"+propSet);
	    		//System.out.println("MAPPING_SCHEMA ***********"+MAPPING_SCHEMA);
	    		if(propSet.contains(list[0].trim()))
	    		{
	    			if(!map.containsKey(list[0].trim()))
	    			{
	    				map.put(list[0].trim(),list[1].trim());
	    			}
	    			flag = 1;
	    		}
	    	}
	    	if(flag==1)
	    	{
	    		record.put("s", triple[0].trim());
	    		for(HashMap.Entry<String,String> entry : map.entrySet())
	    		{
	    			String t = entry.getKey().trim();
	    			t = t.startsWith("<") && t.endsWith(">")
	         				? t.substring(1, t.length() - 1).replaceAll("[[^\\w]+]", "_")
	         				: t.replaceAll("[[^\\w]+]", "_");
	    			record.put("o",entry.getValue());
	    		}
	    		for(String t : propSet)
	    		{
	    			if(!map.containsKey(t))
	    			{
		    			t = t.startsWith("<") && t.endsWith(">")
		         				? t.substring(1, t.length() - 1).replaceAll("[[^\\w]+]", "_")
		         				: t.replaceAll("[[^\\w]+]", "_");
	    				record.put("o",null);
	    			}
	    		}
	    		//System.out.println("YES WE DID IT++++++++++"+record);
	    		context.write(null, record); 
	    	}
	    		
	    }             
	}
	
	
	@Override
    public int run(String[] args) throws Exception
    {
    	Configuration conf = new Configuration();
    	String uri = args[2];
    	FileSystem fsi = FileSystem.get(URI.create(uri) , conf);
    	InputStream in = fsi.open(new Path(uri));
    	BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line = null;
        ArrayList<String> naryPropTableList = new ArrayList<String>();
        while ((line = br.readLine()) != null) 
        {
        	String temp[] = line.split(" ");
        	if(temp.length==1)
        		naryPropTableList.add(line.trim());
        }
        
        String[] naryPropTable = new String[naryPropTableList.size()];
        naryPropTable=naryPropTableList.toArray(naryPropTable);
        /*
        uri = "hdfs://localhost:54310/ritesh/output/relationalTable/wide_property_table" ;
		fsi = FileSystem.get(URI.create(uri) , conf);
	    if (fsi.exists( new Path(uri) )) 
	    {
	    	fsi.delete(new Path(uri), true);
	    }	
	    */
	    for(String x : naryPropTable)
        {
	    	String s = "    {\"name\":\"s\", \"type\":\"string\"},\n";
	    	//"{\"name\":    \"line\", \"type\":    \"string\"}\n"
        	String t = x.trim();
    		t = t.startsWith("<") && t.endsWith(">")
    				? t.substring(1, t.length() - 1).replaceAll("[[^\\w]+]", "_")
    				: t.replaceAll("[[^\\w]+]", "_");
    		s=s+"    {\"name\":\"o\", \"type\":[\"null\",\"string\"]}\n";	
        	String sc = "{\n" +
                    "    \"type\":\"record\",\n" +                
                    "    \"name\":\"TextFile\",\n" +
                    "    \"doc\":\"Text File\",\n" +
                    "    \"fields\":\n" + 
                    "    [\n" +
                    		s +
                    "    ]\n"+
                    "}\n";
        	
        	//final String[] po = input.getString(0).split(columns_separator);
        	System.out.println("----------========"+sc+"%%%%%%%%%%$$$$$$$$$$$$");
            //conf= new Configuration();
            MAPPING_SCHEMA = new Schema.Parser().parse(sc);
        	//GenericRecord record = new GenericData.Record(MAPPING_SCHEMA);
        	propSet = new HashSet( Arrays.asList(x.trim().split(" ")) );
        	//System.out.println("propSet in driver ***********"+propSet);
        	//Gson gson = new Gson();
        	//String setSerialization1 = gson.toJson(propSet);
            //conf.set("propSet", setSerialization1);
            //setSerialization1 = gson.toJson(MAPPING_SCHEMA);
            //conf.set("MAPPING_SCHEMA", setSerialization1);
            //setSerialization1 = gson.toJson(record);
            //conf.set("record", setSerialization1);
        	Job job_makeParquet = Job.getInstance(getConf(), "ParquetConvert");//new Job(conf, "Convert into parquet format");
        	job_makeParquet.setJarByClass(getClass());
        	job_makeParquet.setMapperClass(ParquetConvertMapper.class);   
        	//job_makeParquet.setReducerClass(ParquetConvertReducer.class);
        	job_makeParquet.setNumReduceTasks(0);
        	job_makeParquet.setOutputKeyClass(Void.class);
        	job_makeParquet.setOutputValueClass(Group.class);
        	job_makeParquet.setOutputFormatClass(AvroParquetOutputFormat.class);
            // setting schema
            AvroParquetOutputFormat.setSchema(job_makeParquet, MAPPING_SCHEMA);
            FileInputFormat.addInputPath(job_makeParquet, new Path(args[0]));
            FileOutputFormat.setOutputPath(job_makeParquet, new Path(new String(args[1]+"/vp_"+t)));
            job_makeParquet.waitForCompletion(true);
            propSet.clear();
            //break;
        }
	    return 0;
    }
	
	public static void main(String[] args) throws Exception
	{
        int exitFlag = ToolRunner.run(new ParquetConvert(), args);
        System.exit(exitFlag);
    }

} 
