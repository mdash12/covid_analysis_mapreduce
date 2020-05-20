import java.io.*;
import java.util.*;
import java.lang.*;
import java.net.URI;
import java.util.Date;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.fs.FileSystem; 

public class Covid19_3 {
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	public static class MyMapper extends Mapper<Object, Text, Text, DoubleWritable> {
		private static DoubleWritable new_cases = new DoubleWritable(0);
		private Text location = new Text();

		// The 4 types declared here should match the types that was declared on the top
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer tok = new StringTokenizer(value.toString(), ","); 
			
			try{
				if(tok.hasMoreTokens())
					tok.nextToken();
				if(tok.hasMoreTokens())
					location.set(tok.nextToken());
				if(tok.hasMoreTokens())
					new_cases.set(Long.parseLong(tok.nextToken()));

				context.write(location, new_cases);
			
			}catch(Exception e){
				
			}
		}
		
	}
	
	// 4 types declared: Type of input key, type of input value, type of output key, type of output value
	// The input types of reduce should match the output type of map
	public static class MyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
		private DoubleWritable pop_perm = new DoubleWritable(0.0);
		HashMap<String,Long> pop_table = new HashMap<>();

		public void setup(Context context) throws IOException,  InterruptedException{

		    URI[] files = context.getCacheFiles();
			FileSystem fs = FileSystem.get(new Configuration());
		    Path popfile = new Path(files[0].toString());
		    BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(popfile)));

		    String l = "";
		    while((l=br.readLine())!=null){
		    	String cols[] = l.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1);
		    	try{
		    		pop_table.put(cols[1],Long.parseLong(cols[4]));
		    	}catch(Exception e){
		    		
		    	}
		    }
		}
		
		// Notice the that 2nd argument: type of the input value is an Iterable collection of objects 
		//  with the same type declared above/as the type of output value from map
		public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
			long sum = 0;
			for (DoubleWritable tmp: values) {
				sum += tmp.get();
			}
			try{
				long tot_pop = pop_table.get(key.toString());
				if(tot_pop>0){
					double pop_pm = (sum *1.0 /tot_pop)*1000000;
					// This write to the final output
					pop_perm.set(pop_pm);
					context.write(key, pop_perm);
				}
			}catch(Exception e){
				
			}
			
		}
	}
	
	
	public static void main(String[] args)  throws Exception {

		long startTime  = new Date().getTime();
		Configuration conf = new Configuration();
		Job myjob = Job.getInstance(conf, "Covid19_3 task");
		myjob.setJarByClass(Covid19_3.class);
		myjob.setMapperClass(MyMapper.class);
		myjob.setReducerClass(MyReducer.class);
		myjob.setOutputKeyClass(Text.class);
		myjob.setOutputValueClass(DoubleWritable.class);
		myjob.addCacheFile(new Path(args[1]).toUri());
		
		// Uncomment to set the number of reduce tasks
		// myjob.setNumReduceTasks(2);
		FileInputFormat.addInputPath(myjob, new Path(args[0]));
		FileOutputFormat.setOutputPath(myjob,  new Path(args[2]));
		boolean status = myjob.waitForCompletion(true);   
		long endTime = new Date().getTime();
		double executionTime = (endTime - startTime)/1000.0;
		System.out.println("Execution time: " + executionTime + "seconds");
		System.exit(status ? 0 : 1);
	}
}