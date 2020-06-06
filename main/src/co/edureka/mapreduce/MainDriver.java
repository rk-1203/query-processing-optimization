/*Usage : arg[0]:inpput path of Dataset
 * 		  arg[1]:output path of Property Usage
 * 		  arg[2]:output path of Subject Property Bucket
 * 		  arg[3]:output path of Property Cluster
 * 		  arg[4]:output path of Cluster count
 * 		  arg[5]:output path of Cluster
 * 		  arg[6]:output path of Table
 * 		  arg[7]:output path of extended table after clustering
 * 		  arg[8]:output path of table after patitioning (intitial partitioning)
 * 		  arg[9]:output path of final Table
 * 		  arg[10]:output path of final extended table after clustering
 * 		  arg[11]:output path of final table after patitioning (intitial partitioning)
 * 		  arg[12]:output path of final merged table
 * 
 * op_table1		: Properties which are not in the cluster(spb having greater support value than threshold)
 * op_cluster		: cluster which has greater support value than  support threshold
 * op_table2		: cluster whose null% is less than the nullThreshold- and has greater support value than  support threshold
 * op_cluster_temp	: cluster having SV > SThreshold and null% > nullThreshold
 * 
 */ 
package co.edureka.mapreduce;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.example.data.Group;

import com.google.gson.Gson;

public class MainDriver 
{
	final static int SUPPORT_THRESHOLD = 1;
	final static double NULL_THRESHOLD = 30;
	final static String[][] PREFIX = new String[][] {
		{ "rdf", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#" },
		{ "xsd", "<http://www.w3.org/2001/XMLSchema#" },
		{ "rdfs", "<http://www.w3.org/2000/01/rdf-schema#" },
		{ "owl", "<http://www.w3.org/2002/07/owl#" },
		{ "foaf", "<http://xmlns.com/foaf/0.1/" },
		{ "foaf", "<http://xmlns.com/foaf/" },
		{ "bench", "<http://localhost/vocabulary/bench/" },
		{ "person", "<http://localhost/persons/" },
		{ "ub", "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#" },
		{ "bsbm-export", "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/export/" },
		{ "bsbm", "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/" },
		{ "bsbm-inst", "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/" },
		{ "dc", "<http://purl.org/dc/elements/1.1/" },
		{ "dcterms", "<http://purl.org/dc/terms/" },
		{ "dctype", "<http://purl.org/dc/dcmitype/" },
		{ "rev", "<http://purl.org/stuff/rev#" },
		{ "rss", "<http://purl.org/rss/1.0/" },
		{ "gr", "<http://purl.org/goodrelations/" },
		{ "mo", "<http://purl.org/ontology/mo/" },
		{ "swrc", "<http://swrc.ontoware.org/ontology#" },
		{ "ex", "<http://example.org/" },
		{ "gn", "<http://www.geonames.org/ontology#" },
		{ "og", "<http://ogp.me/ns#" },
		{ "sorg", "<http://schema.org/" },
		{ "wsdbm", "<http://db.uwaterloo.ca/~galuc/wsdbm/" }
};
	
	public static double nullPer(String propList,HashMap<String,Double> propUsage)
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
	
	public static void main(String[] args) throws Exception 
	{
	 
		Configuration conf= new Configuration();
		Job job_prop_usage = new Job(conf,"Generate Property Usage Table");
		job_prop_usage.setJarByClass(MainDriver.class);
		job_prop_usage.setMapperClass(MapPropUsage.class);
		job_prop_usage.setReducerClass(ReducePropUsage.class);
		job_prop_usage.setOutputKeyClass(Text.class);
		job_prop_usage.setOutputValueClass(IntWritable.class);
		job_prop_usage.setInputFormatClass(TextInputFormat.class);
		job_prop_usage.setOutputFormatClass(TextOutputFormat.class);
		Path outputPath = new Path(args[1]);
		//Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job_prop_usage, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_prop_usage, new Path(args[1]));
		//deleting the output path automatically from hdfs so that we don't have to delete it explicitly
		outputPath.getFileSystem(conf).delete(outputPath);
		//System.exit(job_prop_usage.waitForCompletion(true) ? 0 : 1);
		
		if(job_prop_usage.waitForCompletion(true)==false)
			System.exit(1);
		
		conf= new Configuration();
		Job job_sub_prop_bucket = new Job(conf,"Generate Subject Property Bucket");
		job_sub_prop_bucket.setJarByClass(MainDriver.class);
		job_sub_prop_bucket.setMapperClass(MapSubPropBucket.class);
		job_sub_prop_bucket.setReducerClass(ReduceSubPropBucket.class);
		job_sub_prop_bucket.setOutputKeyClass(Text.class);
		job_sub_prop_bucket.setOutputValueClass(Text.class);
		job_sub_prop_bucket.setInputFormatClass(TextInputFormat.class);
		job_sub_prop_bucket.setOutputFormatClass(TextOutputFormat.class);
		outputPath = new Path(args[2]);
		//Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job_sub_prop_bucket, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_sub_prop_bucket, new Path(args[2]));
		//deleting the output path automatically from hdfs so that we don't have to delete it explicitly
		outputPath.getFileSystem(conf).delete(outputPath);
		
		if(job_sub_prop_bucket.waitForCompletion(true)==false)
			System.exit(1);
		
		conf= new Configuration();
		Job job_makeCluster = new Job(conf,"Create Property cluster count");
		job_makeCluster.setJarByClass(MainDriver.class);
		job_makeCluster.setMapperClass(MapPropCluster.class);
		job_makeCluster.setReducerClass(ReducePropCluster.class);
		job_makeCluster.setOutputKeyClass(Text.class);
		job_makeCluster.setOutputValueClass(IntWritable.class);
		job_makeCluster.setInputFormatClass(TextInputFormat.class);
		job_makeCluster.setOutputFormatClass(TextOutputFormat.class);
		outputPath = new Path(args[3]);
		//Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job_makeCluster, new Path(args[2]));
		FileOutputFormat.setOutputPath(job_makeCluster, new Path(args[3]));
		//deleting the output path automatically from hdfs so that we don't have to delete it explicitly
		outputPath.getFileSystem(conf).delete(outputPath);
		
		if(job_makeCluster.waitForCompletion(true)==false)
			System.exit(1);
		
		HashMap<String,Double> propMap = new HashMap<String,Double>();
		conf = new Configuration();
		String uri = args[3]+"/part-r-00000" ;
		FileSystem fsi = FileSystem.get(URI.create(uri) , conf);
		InputStream in = fsi.open(new Path(uri));
		BufferedReader br = new BufferedReader(new InputStreamReader(in));
        String line = null;
        while((line = br.readLine()) != null)
        {
            String[] temp = line.trim().split("\t");
            //HashSet<String> set = new HashSet<String>(Arrays.asList(temp[0].trim().split(" ")));
            propMap.put(temp[0].trim(),Double.parseDouble(temp[1].trim()));
        }
        conf= new Configuration();
		Gson gson = new Gson();
        String setSerialization = gson.toJson(propMap);
        conf.set("propMap", setSerialization);
        Job job_cluster_set_count = new Job(conf,"Count the subsets of the cluster");
		job_cluster_set_count.setJarByClass(MainDriver.class);
		job_cluster_set_count.setMapperClass(MapClusterSetCount.class);
		job_cluster_set_count.setReducerClass(ReduceClusterSetCount.class);
		job_cluster_set_count.setOutputKeyClass(Text.class);
		job_cluster_set_count.setOutputValueClass(LongWritable.class);
		job_cluster_set_count.setInputFormatClass(TextInputFormat.class);
		job_cluster_set_count.setOutputFormatClass(TextOutputFormat.class);
		outputPath = new Path(args[3]+"_temp");
		//Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job_cluster_set_count, new Path(args[3]));
		FileOutputFormat.setOutputPath(job_cluster_set_count, outputPath);
		//deleting the output path automatically from hdfs so that we don't have to delete it explicitly
		outputPath.getFileSystem(conf).delete(outputPath);
		//System.exit(job_cluster_set_counts.waitForCompletion(true) ? 0 : 1);
		
		if(job_cluster_set_count.waitForCompletion(true)==false)
			System.exit(1);
		
		
		uri = args[3];
		conf= new Configuration();
		FileSystem fs = FileSystem.get(URI.create(uri),conf);
		// delete existing directory
		fs.delete(new Path(uri),true);
		
		conf = new Configuration();
	    String str = args[3]+"_temp";
	    String dst = args[3];
	    fs = FileSystem.get(URI.create(str), conf);
	    Path srcPath = new Path(str);
	    Path dstPath = new Path(dst);
	    fs.rename(srcPath, dstPath);
	    fs.close();
		//System.exit(1);
	    
		conf= new Configuration();
		Job job_cluster_count = new Job(conf,"Generate Total Number of Cluster");
		job_cluster_count.setJarByClass(MainDriver.class);
		job_cluster_count.setMapperClass(MapClusterCount.class);
		job_cluster_count.setReducerClass(ReduceClusterCount.class);
		job_cluster_count.setOutputKeyClass(Text.class);
		job_cluster_count.setOutputValueClass(LongWritable.class);
		job_cluster_count.setInputFormatClass(TextInputFormat.class);
		job_cluster_count.setOutputFormatClass(TextOutputFormat.class);
		outputPath = new Path(args[4]);
		System.out.println("-------------------------Driver of clusterCount******************");
		//Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job_cluster_count, new Path(args[3]));
		FileOutputFormat.setOutputPath(job_cluster_count, outputPath);
		//deleting the output path automatically from hdfs so that we don't have to delete it explicitly
		outputPath.getFileSystem(conf).delete(outputPath);
		//System.exit(job_cluster_count.waitForCompletion(true) ? 0 : 1);
		
		if(job_cluster_count.waitForCompletion(true)==false)
			System.exit(1);
		
		Configuration conf1 = new Configuration();
		uri = args[4]+"/part-r-00000" ;
		fsi = FileSystem.get(URI.create(uri) , conf1);
		in = fsi.open(new Path(uri));
		//System.out.println(tmp[0]);
		//IOUtils.copy(in, writer, StandardCharsets.UTF_8);
		///String[] tmp = (writer.toString()).split("\t");
		String[] tmp = (IOUtils.toString(in,StandardCharsets.UTF_8)).split("\t");
		int totalPropCluster = Integer.parseInt(tmp[1].trim());
		System.out.println(totalPropCluster+"**************$$$$$$$$------------------These are the clusters generated ----------$$$$$$$$$$$$$$$**********");
		//IOUtils.closeStream(in);
		
		conf= new Configuration();
		conf.setInt("SUPPORT_THRESHOLD", SUPPORT_THRESHOLD);
		conf.setInt("totalPropCluster", totalPropCluster);
		Job job_cluster = new Job(conf,"Generate Support of Cluster and Perform sorting and Generate final Cluster list");
		job_cluster.setJarByClass(MainDriver.class);
		job_cluster.setMapperClass(MapCluster.class);
		job_cluster.setReducerClass(ReduceCluster.class);
		job_cluster.setOutputKeyClass(DoubleWritable.class);
		job_cluster.setOutputValueClass(Text.class);
		job_cluster.setInputFormatClass(TextInputFormat.class);
		job_cluster.setOutputFormatClass(TextOutputFormat.class);
		outputPath = new Path(args[5]);
		System.out.println("-------------------------Driver of clusterCount******************");
		//Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job_cluster, new Path(args[3]));
		FileOutputFormat.setOutputPath(job_cluster, outputPath);
		//deleting the output path automatically from hdfs so that we don't have to delete it explicitly
		outputPath.getFileSystem(conf).delete(outputPath);
		//System.exit(job_cluster.waitForCompletion(true) ? 0 : 1);
		
		if(job_cluster.waitForCompletion(true)==false)
			System.exit(1);
		
		
		HashSet<String> hashset = new HashSet<String>();
		conf = new Configuration();
		uri = args[5]+"/part-r-00000" ;
		fsi = FileSystem.get(URI.create(uri) , conf);
		in = fsi.open(new Path(uri));
		br = new BufferedReader(new InputStreamReader(in));
        line = null;
        while ((line = br.readLine()) != null) 
        {
            String[] temp = line.trim().split("\t");
            temp = temp[1].trim().split(" ");
            for(String x : temp)
            	hashset.add(x.trim());
        }
        System.out.print("Hashset = .........................................................................................................................\n"+hashset);
		
        conf= new Configuration();
		gson = new Gson();
        setSerialization = gson.toJson(hashset);
        conf.set("hashset", setSerialization);
		Job job_table = new Job(conf,"Generate Tables");
		job_table.setJarByClass(MainDriver.class);
		job_table.setMapperClass(MapTable.class);
		job_table.setReducerClass(ReduceTable.class);
		job_table.setOutputKeyClass(Text.class);
		job_table.setOutputValueClass(IntWritable.class);
		job_table.setInputFormatClass(TextInputFormat.class);
		job_table.setOutputFormatClass(TextOutputFormat.class);
		outputPath = new Path(args[6]);
		//Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job_table, new Path(args[1]));
		FileOutputFormat.setOutputPath(job_table, outputPath);
		//deleting the output path automatically from hdfs so that we don't have to delete it explicitly
		outputPath.getFileSystem(conf).delete(outputPath);
		//System.exit(job_tables.waitForCompletion(true) ? 0 : 1);
		
		if(job_table.waitForCompletion(true)==false)
			System.exit(1);
		
		
		//Reading op_pu and putting it in map
		HashMap<String, Double> propUsage = new HashMap<String, Double>(); 
		conf1 = new Configuration();
		uri = args[1]+"/part-r-00000" ;
		fsi = FileSystem.get(URI.create(uri) , conf1);
		in = fsi.open(new Path(uri));
		br = new BufferedReader(new InputStreamReader(in));
        line = null;
        while ((line = br.readLine()) != null) 
        {
            String[] temp = line.trim().split("\t");
            propUsage.put( temp[0].trim(),Double.parseDouble(temp[1].trim()) );
            
        }
        
        System.out.println("++++++++++_______*******--------"+propUsage);
        //reading op_cluster and storing the count of each properties
        HashMap<String, Double> tempMap = new HashMap<String, Double>(); 
		conf1 = new Configuration();
		uri = args[5]+"/part-r-00000" ;
		fsi = FileSystem.get(URI.create(uri) , conf1);
		in = fsi.open(new Path(uri));
		br = new BufferedReader(new InputStreamReader(in));
        line = null;
        while ((line = br.readLine()) != null) 
        {
            String[] temp = line.trim().split("\t");
            String[] property = temp[1].trim().split(" ");
            for( String x : property)
            {
            	if(tempMap.containsKey(x))
            		tempMap.put(x,new Double(tempMap.get(x)+1.0));
            	else
            		tempMap.put(x,new Double(1.0));
            }
            
        }
        
		
		conf= new Configuration();
		
		gson = new Gson();
        String setSerialization1 = gson.toJson(propUsage);
        conf.set("propUsage", setSerialization1);
        
        String setSerialization2 = gson.toJson(tempMap);
        conf.set("tempMap", setSerialization2);
        
        conf.setDouble("NULL_THRESHOLD", NULL_THRESHOLD);
        
		Job job_extendTable = new Job(conf,"Extend Tables");
		job_extendTable.setJarByClass(MainDriver.class);
		job_extendTable.setMapperClass(MapExtendTable.class);
		job_extendTable.setReducerClass(ReduceExtendTable.class);
		job_extendTable.setOutputKeyClass(Text.class);
		job_extendTable.setOutputValueClass(IntWritable.class);
		job_extendTable.setInputFormatClass(TextInputFormat.class);
		job_extendTable.setOutputFormatClass(TextOutputFormat.class);
		outputPath = new Path(args[7]);
		System.out.println("-------------------------Driver of clusterCount******************");
		//Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job_extendTable, new Path(args[5]));
		FileOutputFormat.setOutputPath(job_extendTable, outputPath);
		//deleting the output path automatically from hdfs so that we don't have to delete it explicitly
		outputPath.getFileSystem(conf).delete(outputPath);
		//System.exit(job_tables.waitForCompletion(true) ? 0 : 1);
		if(job_extendTable.waitForCompletion(true)==false)
			System.exit(1);
		
		HashMap<String,Double> table_cluster = new HashMap<String,Double>();
		conf = new Configuration();
		uri = args[7]+"/part-r-00000" ;
		fsi = FileSystem.get(URI.create(uri) , conf);
		in = fsi.open(new Path(uri));
		br = new BufferedReader(new InputStreamReader(in));
        line = null;
        while ((line = br.readLine()) != null) 
        {
            String[] temp = line.trim().split("\t");
            //temp[1] = temp[1].trim();
            table_cluster.put(temp[1].trim(),Double.parseDouble(temp[0].trim()));
        }
        gson = new Gson();
        setSerialization1 = gson.toJson(propUsage);
        conf.set("table_cluster", setSerialization1);
        
        Job job_reduce_cluster = new Job(conf,"Reduce Cluster");
		job_reduce_cluster.setJarByClass(MainDriver.class);
		job_reduce_cluster.setMapperClass(MapReduceCluster.class);
		job_reduce_cluster.setReducerClass(ReduceReduceCluster.class);
		job_reduce_cluster.setOutputKeyClass(DoubleWritable.class);
		job_reduce_cluster.setOutputValueClass(Text.class);
		job_reduce_cluster.setInputFormatClass(TextInputFormat.class);
		job_reduce_cluster.setOutputFormatClass(TextOutputFormat.class);
		outputPath = new Path(args[5]+"_temp");
		FileInputFormat.addInputPath(job_reduce_cluster, new Path(args[5]));
		FileOutputFormat.setOutputPath(job_reduce_cluster, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath);
		
		if(job_reduce_cluster.waitForCompletion(true)==false)
			System.exit(1);
		
		tempMap.clear();
		tempMap = new HashMap<String, Double>(); 
		conf1 = new Configuration();
		uri = args[5]+"_temp/part-r-00000" ;
		fsi = FileSystem.get(URI.create(uri) , conf1);
		in = fsi.open(new Path(uri));
		br = new BufferedReader(new InputStreamReader(in));
        line = null;
        while ((line = br.readLine()) != null) 
        {
            String[] temp = line.trim().split("\t");
            String[] property = temp[1].trim().split(" ");
            for( String x : property)
            {
            	if(tempMap.containsKey(x))
            		tempMap.put(x,new Double(tempMap.get(x)+1.0));
            	else
            		tempMap.put(x,new Double(1.0));
            }
            
        }
        
        /*conf= new Configuration();
		gson = new Gson();
        setSerialization1 = gson.toJson(propUsage);
        conf.set("propUsage", setSerialization1);
        
        setSerialization2 = gson.toJson(tempMap);
        conf.set("tempMap", setSerialization2);
        
        conf.setDouble("NULL_THRESHOLD", NULL_THRESHOLD);
        
		Job job_partition = new Job(conf,"Patitioning");
		job_partition.setJarByClass(MainDriver.class);
		job_partition.setMapperClass(MapPartition.class);
		job_partition.setReducerClass(ReducePartition.class);
		job_partition.setOutputKeyClass(Text.class);
		job_partition.setOutputValueClass(IntWritable.class);
		job_partition.setInputFormatClass(TextInputFormat.class);
		job_partition.setOutputFormatClass(TextOutputFormat.class);
		outputPath = new Path(args[8]);
		System.out.println("-------------------------Driver of clusterCount******************");
		//Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job_partition, new Path(args[5]+"_temp"));
		FileOutputFormat.setOutputPath(job_partition, outputPath);
		//deleting the output path automatically from hdfs so that we don't have to delete it explicitly
		outputPath.getFileSystem(conf).delete(outputPath);
		//System.exit(job_tables.waitForCompletion(true) ? 0 : 1);
		if(job_partition.waitForCompletion(true)==false)
			System.exit(1);
        */
        //conf = new Configuration();
		uri = args[5]+"_temp/part-r-00000" ;
		fsi = FileSystem.get(URI.create(uri) , conf);
		in = fsi.open(new Path(uri));
		br = new BufferedReader(new InputStreamReader(in));
        line = null;
       
        conf = new Configuration();
		uri = args[8] ;
		fsi = FileSystem.get(URI.create(uri) , conf);
	    if (fsi.exists( new Path(args[8]) )) 
	    {
	    	fsi.delete(new Path(args[8]), true);
	    }		
        FSDataOutputStream fs_append = fsi.create(new Path (uri));
        PrintWriter writer = new PrintWriter(fs_append);
        //writer.append("Prince raj");
        ArrayList<String> cluster_list = new ArrayList<String>();
        
        while ((line = br.readLine()) != null) 
        {
            cluster_list.add(line.trim());
        }
        String[] cluster = new String[cluster_list.size()];
        cluster=cluster_list.toArray(cluster);
        for(int i=0;i<cluster.length;i++)
        {
            String[] temp = cluster[i].trim().split("\t");
            while( nullPer(temp[1].trim(),propUsage) > NULL_THRESHOLD )
    		{
    			String prop[] = temp[1].trim().split(" ");
    			String minProp=null;
    			for(String x : prop)
    			{
    				if(minProp==null || propUsage.get(x)<propUsage.get(minProp))
    					minProp = x;
    			}
    			temp[1] = temp[1].replace(" "+minProp+" ", " ");
    			temp[1] = temp[1].replace(minProp+" ", "");
    			temp[1] = temp[1].replace(" "+minProp, "");
    			double v = tempMap.get(minProp);
    			long x = (long)v;
    			if(tempMap.containsKey(minProp) && x==1)
    			{
    				writer.append(minProp+"\n");
    			}
    			tempMap.put(minProp,tempMap.get(minProp)-1);
    		}
            writer.append(temp[1].trim()+"\n");
            for(String p : temp[1].split(" ") )
            {
            	for(int j=i+1;j<cluster.length;j++)
            	{
            		cluster[j] = cluster[j].replace(" "+p+" ", " ");
            		cluster[j] = cluster[j].replace(p+" ", "");
            		cluster[j] = cluster[j].replace(" "+p, "");
            	}
            }
        }
        writer.flush();
        fs_append.hflush();
        writer.close();
        fs_append.close();
        
        conf = new Configuration();
		Job job_removeDuplicates = new Job(conf,"Remove_duplicates from op_table1");
		job_removeDuplicates.setJarByClass(MainDriver.class);
		job_removeDuplicates.setMapperClass(MapRemoveDuplicates.class);
		job_removeDuplicates.setReducerClass(ReduceRemoveDuplicates.class);
		job_removeDuplicates.setOutputKeyClass(Text.class);
		job_removeDuplicates.setOutputValueClass(IntWritable.class);
		job_removeDuplicates.setInputFormatClass(TextInputFormat.class);
		job_removeDuplicates.setOutputFormatClass(TextOutputFormat.class);
		outputPath = new Path(args[9]);
		System.out.println("-------------------------Driver of clusterCount******************");
		//Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job_removeDuplicates, new Path(args[6]));
		FileOutputFormat.setOutputPath(job_removeDuplicates, outputPath);
		//deleting the output path automatically from hdfs so that we don't have to delete it explicitly
		outputPath.getFileSystem(conf).delete(outputPath);
		//System.exit(job_removeDuplicates.waitForCompletion(true) ? 0 : 1);
		
		if(job_removeDuplicates.waitForCompletion(true)==false)
			System.exit(1);
		
		conf = new Configuration();
		job_removeDuplicates = new Job(conf,"Remove_duplicates from op_table2");
		job_removeDuplicates.setJarByClass(MainDriver.class);
		job_removeDuplicates.setMapperClass(MapRemoveDuplicates.class);
		job_removeDuplicates.setReducerClass(ReduceRemoveDuplicates.class);
		job_removeDuplicates.setOutputKeyClass(Text.class);
		job_removeDuplicates.setOutputValueClass(IntWritable.class);
		job_removeDuplicates.setInputFormatClass(TextInputFormat.class);
		job_removeDuplicates.setOutputFormatClass(TextOutputFormat.class);
		outputPath = new Path(args[10]);
		System.out.println("-------------------------Driver of clusterCount******************");
		//Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job_removeDuplicates, new Path(args[7]));
		FileOutputFormat.setOutputPath(job_removeDuplicates, outputPath);
		//deleting the output path automatically from hdfs so that we don't have to delete it explicitly
		outputPath.getFileSystem(conf).delete(outputPath);
		//System.exit(job_removeDuplicates.waitForCompletion(true) ? 0 : 1);
		
		if(job_removeDuplicates.waitForCompletion(true)==false)
			System.exit(1);
		
		conf = new Configuration();
		job_removeDuplicates = new Job(conf,"Remove_duplicates from op_table3");
		job_removeDuplicates.setJarByClass(MainDriver.class);
		job_removeDuplicates.setMapperClass(MapRemoveDuplicates.class);
		job_removeDuplicates.setReducerClass(ReduceRemoveDuplicates.class);
		job_removeDuplicates.setOutputKeyClass(Text.class);
		job_removeDuplicates.setOutputValueClass(IntWritable.class);
		job_removeDuplicates.setInputFormatClass(TextInputFormat.class);
		job_removeDuplicates.setOutputFormatClass(TextOutputFormat.class);
		outputPath = new Path(args[11]);
		System.out.println("-------------------------Driver of clusterCount******************");
		//Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job_removeDuplicates, new Path(args[8]));
		FileOutputFormat.setOutputPath(job_removeDuplicates, outputPath);
		//deleting the output path automatically from hdfs so that we don't have to delete it explicitly
		outputPath.getFileSystem(conf).delete(outputPath);
		//System.exit(job_removeDuplicates.waitForCompletion(true) ? 0 : 1);
		
		if(job_removeDuplicates.waitForCompletion(true)==false)
			System.exit(1);
		
		       
        conf = new Configuration();
		uri = args[12] ;
		fsi = FileSystem.get(URI.create(uri) , conf);
	    if (fsi.exists( new Path(args[12]) )) 
	    {
	    	fsi.delete(new Path(args[12]), true);
	    }		
        fs_append = fsi.create(new Path (uri));
        writer = new PrintWriter(fs_append);
        //writer.append("Prince raj");
        
        ArrayList<String> propClusterTableList = new ArrayList<String>();
        
        uri = args[6]+"/part-r-00000";
		fsi = FileSystem.get(URI.create(uri) , conf);
		in = fsi.open(new Path(uri));
		br = new BufferedReader(new InputStreamReader(in));
        line = null;
        while ((line = br.readLine()) != null) 
        {
        	writer.append(line.split("\t")[0].trim()+"\n");
        	propClusterTableList.add(line.split("\t")[0].trim());
        }
        
        uri = args[7]+"/part-r-00000";
		fsi = FileSystem.get(URI.create(uri) , conf);
		in = fsi.open(new Path(uri));
		br = new BufferedReader(new InputStreamReader(in));
        line = null;
        while ((line = br.readLine()) != null) 
        {
        	writer.append(line.split("\t")[0].trim()+"\n");
        	propClusterTableList.add(line.split("\t")[0].trim());
        }
        
        
        uri = args[8];
		fsi = FileSystem.get(URI.create(uri) , conf);
		in = fsi.open(new Path(uri));
		br = new BufferedReader(new InputStreamReader(in));
        line = null;
        while ((line = br.readLine()) != null) 
        {
        	writer.append(line+"\n");
        	propClusterTableList.add(line.split("\t")[0].trim());
        }
        writer.flush();
        fs_append.hflush();
        writer.close();
        fs_append.close();
        
        String[] propClusterTable = new String[propClusterTableList.size()];
        propClusterTable=propClusterTableList.toArray(propClusterTable);
        
        uri = "hdfs://master:54310/ritesh/output/relationalTable/binary_table" ;
		fsi = FileSystem.get(URI.create(uri) , conf);
	    if (fsi.exists( new Path(uri) )) 
	    {
	    	fsi.delete(new Path(uri), true);
	    }	
        
        for(String x : propClusterTable)
        {
        	String temp[] = x.split(" ");
        	if(temp.length!=1)
        		continue;
        	String prefix="",opFileName;
        	for(String[] pattern : PREFIX)
        	{
        		if(x.contains(pattern[1]))
        		{
        			prefix = pattern[0];
        			break;
        		}
        	}
        	opFileName = prefix+"_"+x.substring(x.lastIndexOf('#') + 1 ,x.length()-1);
        	conf = new Configuration();
        	gson = new Gson();
            setSerialization1 = gson.toJson(x);
            conf.set("prop", setSerialization1);
    		Job job_makeTable = new Job(conf,"Implement final cluters in files");
    		job_makeTable.setJarByClass(MainDriver.class);
    		job_makeTable.setMapperClass(MapMakeTable.class);
    		job_makeTable.setReducerClass(ReduceMakeTable.class);
    		job_makeTable.setOutputKeyClass(Text.class);
    		job_makeTable.setOutputValueClass(Text.class);
    		job_makeTable.setInputFormatClass(TextInputFormat.class);
    		job_makeTable.setOutputFormatClass(TextOutputFormat.class);
    		outputPath = new Path("hdfs://master:54310/ritesh/output/relationalTable/binary_table/"+opFileName);
    		FileInputFormat.addInputPath(job_makeTable, new Path(args[0]));
    		FileOutputFormat.setOutputPath(job_makeTable, outputPath);
    		//deleting the output path automatically from hdfs so that we don't have to delete it explicitly
    		outputPath.getFileSystem(conf).delete(outputPath);
    		//System.exit(job_makeTable.waitForCompletion(true) ? 0 : 1);
    		
    		if(job_makeTable.waitForCompletion(true)==false)
    			System.exit(1);
        }        
        
        conf = new Configuration();
		Job job_makeTextFile = new Job(conf,"Make a text File of Nary Table");
		job_makeTextFile.setJarByClass(MainDriver.class);
		job_makeTextFile.setMapperClass(MapmakeTextFile.class);
		job_makeTextFile.setReducerClass(ReducemakeTextFile.class);
		job_makeTextFile.setOutputKeyClass(Text.class);
		job_makeTextFile.setOutputValueClass(Text.class);
		job_makeTextFile.setInputFormatClass(TextInputFormat.class);
		job_makeTextFile.setOutputFormatClass(TextOutputFormat.class);
		outputPath = new Path(args[13]);
		System.out.println("-------------------------Driver of clusterCount******************");
		//Configuring the input/output path from the filesystem into the job
		FileInputFormat.addInputPath(job_makeTextFile, new Path(args[0]));
		FileOutputFormat.setOutputPath(job_makeTextFile, outputPath);
		//deleting the output path automatically from hdfs so that we don't have to delete it explicitly
		outputPath.getFileSystem(conf).delete(outputPath);
		//System.exit(job_makeTextFile.waitForCompletion(true) ? 0 : 1);
		
		if(job_makeTextFile.waitForCompletion(true)==false)
			System.exit(1);
        
        
        /*
        uri = args[12];
		fsi = FileSystem.get(URI.create(uri) , conf);
		in = fsi.open(new Path(uri));
		br = new BufferedReader(new InputStreamReader(in));
        line = null;
        ArrayList<String> naryPropTableList = new ArrayList<String>();
        while ((line = br.readLine()) != null) 
        {
        	String temp[] = line.split(" ");
        	if(temp.length>1)
        		naryPropTableList.add(line.trim());
        }
        
        String[] naryPropTable = new String[naryPropTableList.size()];
        naryPropTable=naryPropTableList.toArray(naryPropTable);
        
        uri = "hdfs://localhost:54310/ritesh/output/relationalTable/wide_property_table" ;
		fsi = FileSystem.get(URI.create(uri) , conf);
	    if (fsi.exists( new Path(uri) )) 
	    {
	    	fsi.delete(new Path(uri), true);
	    }	
        
	    for(String x : propClusterTable)
        {
	    	String s = "{\"name\":    \"s\", \"type\":    \"string\"},\n";
	    	//"{\"name\":    \"line\", \"type\":    \"string\"}\n"
        	String temp[] = x.split(" ");
        	for( String t : temp)
        	{
        		s=s+"   \"name\":    \""+t+"\", \"type\":    \"string\"},\n";	
        	}
        	s = s.substring(0,s.lastIndexOf(","))+s.substring(s.lastIndexOf(",")+1);
        	Schema MAPPING_SCHEMA = new Schema.Parser().parse
            		(
                    "{\n" +
                    "    \"type\":    \"record\",\n" +                
                    "    \"name\":    \"TextFile\",\n" +
                    "    \"doc\":    \"Text File\",\n" +
                    "    \"fields\":\n" + 
                    "    [\n" + s+
                    //"            {\"name\":    \"line\", \"type\":    \"string\"}\n"+
                    "    ]\n"+
                    "}\n"
                    );
        	conf = new Configuration();
        	gson = new Gson();
            setSerialization1 = gson.toJson(x);
            conf.set("propList", setSerialization1);
            setSerialization1 = gson.toJson(s);
            conf.set("MAPPING_SCHEMA", setSerialization1);
    		
            conf= new Configuration();
        	Job job_makeParquet = new Job(conf, "Convert into parquet format");
        	job_makeParquet.setJarByClass(MainDriver.class);
        	job_makeParquet.setMapperClass(MapParquetConvert.class);   
        	job_makeParquet.setReducerClass(ReduceParquetConvert.class);
        	//job_makeParquet.setNumReduceTasks(0);
        	job_makeParquet.setOutputKeyClass(Void.class);
        	job_makeParquet.setOutputValueClass(Group.class);
        	job_makeParquet.setOutputFormatClass(AvroParquetOutputFormat.class);
            // setting schema
            AvroParquetOutputFormat.setSchema(job_makeParquet, MAPPING_SCHEMA);
            FileInputFormat.addInputPath(job_makeParquet, new Path(args[0]));
            FileOutputFormat.setOutputPath(job_makeParquet, new Path(args[1]));
            job_makeParquet.waitForCompletion(true);
    		if(job_makeParquet.waitForCompletion(true)==false)
    			System.exit(1);
        }
	    */
		System.exit((job_sub_prop_bucket.waitForCompletion(true)&&job_prop_usage.waitForCompletion(true)&&job_makeCluster.waitForCompletion(true))&&job_cluster_count.waitForCompletion(true)&&job_cluster.waitForCompletion(true)&&job_table.waitForCompletion(true)&&job_extendTable.waitForCompletion(true)&&job_reduce_cluster.waitForCompletion(true)&&job_removeDuplicates.waitForCompletion(true) ? 0 : 1);
	}
}
