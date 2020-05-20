import java.io.IOException;
import java.util.*;
import java.lang.*;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Covid19_1 {
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
		private static IntWritable new_cases = new IntWritable();
		private Text location = new Text();
		private String include_world;
		SimpleDateFormat csvDF = new SimpleDateFormat("yyyy-MM-dd");
		
		public void setup(Context context) throws IOException,  InterruptedException{
			Configuration conf = context.getConfiguration();
			include_world = conf.get("World");
		}



		// The 4 types declared here should match the types that was declared on the top
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer tok = new StringTokenizer(value.toString(), ","); 
			
			try{
				Date curDate = new Date();
				Calendar calendar = Calendar.getInstance();
				if(tok.hasMoreTokens())
					curDate=csvDF.parse(tok.nextToken().toString());
				if(tok.hasMoreTokens())
					location.set(tok.nextToken());
				if(tok.hasMoreTokens())
					new_cases = new IntWritable(Integer.parseInt(tok.nextToken()));

				calendar.setTime(curDate);
				int year = calendar.get(Calendar.YEAR);
				if(year==2019)
					return;
				
				if(location.toString().contains("World") || 
					location.toString().contains("International")) {
					if(include_world.equals("true"))
						context.write(location, new_cases);
				}else{
					context.write(location, new_cases);
				}
			}catch(Exception e){
				System.out.println("Exception: " + e);
			}
			
			
		}
		
	}
	
	

	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	// The input types of reduce should match the output type of map
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable total = new IntWritable();
		
		// Notice the that 2nd argument: type of the input value is an Iterable collection of objects 
		//  with the same type declared above/as the type of output value from map
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable tmp: values) {
				sum += tmp.get();
			}
			total.set(sum);
			// This write to the final output
			context.write(key, total);
		}
	}
	
	
	public static void main(String[] args)  throws Exception {
		long startTime  = new Date().getTime();
		Configuration conf = new Configuration();
		conf.set("World",args[1]);
		Job myjob = Job.getInstance(conf, "Covid19_1 task");
		myjob.setJarByClass(Covid19_1.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(IntWritable.class);
		
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[2]));
		boolean status = myjob.waitForCompletion(true);   
		long endTime = new Date().getTime();
		double executionTime = (endTime - startTime)/1000.0;
		System.out.println("Execution time: " + executionTime + " seconds");
		System.exit(status ? 0 : 1);
	}
}