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

public class Covid19_2 {
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
		private static IntWritable new_deaths = new IntWritable();
		private Text location = new Text();
		Date startDate = new Date();
		Date endDate = new Date();
		SimpleDateFormat csvDF = new SimpleDateFormat("yyyy-MM-dd");

		public void setup(Context context) throws IOException,  InterruptedException{
			Configuration conf = context.getConfiguration();
			try{
				startDate = csvDF.parse(conf.get("StartDate").toString());
				endDate = csvDF.parse(conf.get("EndDate").toString());
			}catch(Exception e){

			}
			
		}
		
		// The 4 types declared here should match the types that was declared on the top
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer tok = new StringTokenizer(value.toString(), ","); 
			Date curDate = new Date();

			try{
				if(tok.hasMoreTokens())
					curDate = csvDF.parse(tok.nextToken().toString());
				if(tok.hasMoreTokens())
					location.set(tok.nextToken());
				if(tok.hasMoreTokens())
					tok.nextToken();
				if(tok.hasMoreTokens())
					new_deaths = new IntWritable(Integer.parseInt(tok.nextToken()));

				if(curDate.compareTo(startDate)>=0 && endDate.compareTo(curDate)>=0)
					context.write(location, new_deaths);
				
			}catch(Exception e){
				System.out.println("Exception: "  + e);
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
		conf.set("StartDate",args[1]);
		conf.set("EndDate",args[2]);
		
		
		SimpleDateFormat inputDF = new SimpleDateFormat("yyyy-MM-dd");

		Date validSd = inputDF.parse("2019-12-31");
		Date validEd = inputDF.parse("2020-04-08");
		Date sd = new Date();
		Date ed = new Date();

		try{
			sd = inputDF.parse(conf.get("StartDate").toString());
			ed = inputDF.parse(conf.get("EndDate").toString());

			if(ed.compareTo(sd)<0 || sd.compareTo(validSd)<0 || validEd.compareTo(ed)<0){
				throw new Exception();
			}

		}catch(Exception e){
			System.out.println("Invalid dates entered, exiting");
			System.exit(1);
		}


		
		Job myjob = Job.getInstance(conf, "Covid19_2 task");
		myjob.setJarByClass(Covid19_2.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(IntWritable.class);
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[3]));
		
		boolean status = myjob.waitForCompletion(true);   
		long endTime = new Date().getTime();
		double executionTime = (endTime - startTime)/1000.0;
		System.out.println("Execution time: " + executionTime + " seconds");
		System.exit(status ? 0 : 1);
	}
}