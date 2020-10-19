import java.util.*;
import java.io.IOException;
import java.util.ArrayList;
//import java.util.Iterator;
//import java.lang.*;
//
//import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Context;
import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapred.JobClient;
//import org.apache.hadoop.mapred.JobConf;
//import org.apache.hadoop.util.ToolRunner;
//import org.apache.hadoop.util.Tool;
//import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.MapReduceBase;
//import org.apache.hadoop.mapred.*;

public class equijoin
{


public static class EquijoinMapper extends  Mapper <Object, Text, Text, Text>
{
		
public void map(Object key, Text value, Context con) throws IOException, InterruptedException {
	
	// changing datatype from text to string
	String data[] = value.toString().split(",");
	
	// creating a stringbuilder
	StringBuilder datastrbuilder = new StringBuilder();
	// adding record value to data string builder iteratively
	for(int i=0; i<data.length; i++)
	{
		datastrbuilder.append(data[i].toString().trim() + ",");
	}
	// changing again datatype from stringbuilder to string
	String datastr = datastrbuilder.toString();
	
	if (datastr!=null && datastr.length()>1)
	{
		// reformatting the string
		datastr = datastr.substring(0, datastr.length()-1);
	}
	
	// returning to compiler
	con.write(new Text(data[1].trim()), new Text(datastr));
}
}


public static class EquijoinReducer extends Reducer<Text, Text, Text, Text>
{


	public void reduce(Text key, Iterable<Text> values, Context con) throws IOException, InterruptedException
	{ 
		// initializing variables
		List<String> all_valvs = new ArrayList<String>();
		List<String> tb1 = new ArrayList<String>();
		List<String> tb2 = new ArrayList<String>();
		StringBuilder datastrbuilder = new StringBuilder();
		
		String tb1k = "";
		String tb2k = "";
		
		// importing all values to string
		for (Text valv : values)
		{
			all_valvs.add(valv.toString());
		}
		// getting the table1 name
		tb1k = all_valvs.get(0).split(",")[0];
		for(String x : all_valvs)
		{
			if (tb1k.equals(x.split(",")[0]))
			{
				tb1.add(x);
			}
			else
			{
				tb2.add(x);
			}
		}
		for (String x: tb1)
		{
			for (String y : tb2)
			{
				datastrbuilder.append(x).append(", ").append(y).append("\n");
			}
		}
		Text ans = new Text();
		ans.set(datastrbuilder.toString().trim());
		con.write(null,ans);
		
	}
}
		
		


 







public static void main(String[] args) throws Exception
{


    Configuration cf = new Configuration();
    Job jcf = Job.getInstance(cf, "equijoin");
    jcf.setJarByClass(equijoin.class);
    jcf.setMapOutputKeyClass(Text.class);
    jcf.setMapOutputValueClass(Text.class);
    jcf.setMapperClass(EquijoinMapper.class);
    jcf.setReducerClass(EquijoinReducer.class);

    jcf.setOutputKeyClass(Text.class);
    jcf.setOutputValueClass(Text.class);
    FileInputFormat.setInputPaths(jcf, new Path(args[0]));
    FileOutputFormat.setOutputPath(jcf, new Path(args[1]));
    //JobClient.runJob(jcf);
    System.exit(jcf.waitForCompletion(true) ? 0 : 1);
}
}