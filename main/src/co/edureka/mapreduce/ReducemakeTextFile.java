package co.edureka.mapreduce;


import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public  class ReducemakeTextFile extends  Reducer<Text,Text,Text,Text>  
{
	public void reduce(Text key, Iterable<Text> values, Context output)throws IOException, InterruptedException
	{
		String s = "";
		for(Text x : values)
		{
			if(!s.contains(x.toString()))
				s = s+" "+x.toString();
		}
		output.write(key,new Text(s));
	}
}