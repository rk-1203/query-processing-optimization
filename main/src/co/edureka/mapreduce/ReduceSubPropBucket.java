package co.edureka.mapreduce;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReduceSubPropBucket extends Reducer<Text,Text,Text,Text> 
{
	public void reduce(Text subject,Iterable<Text> predicate, Context context) throws IOException,InterruptedException 
	{
		String predicateList = new String("");
		for(Text x: predicate)
		{
			if(!predicateList.contains(x.toString()))
				predicateList = predicateList + " " + x.toString();
		}
		String[] predicateArr = predicateList.trim().split(" ");
		Arrays.sort(predicateArr);
		predicateList = String.join(" ", predicateArr);
		context.write(subject,new Text(predicateList));
	}
}