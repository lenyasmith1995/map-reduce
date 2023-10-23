package mapreducepackage;

import java.net.URI;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class moviegenrerating extends Configured implements Tool{

	 public static void main(String[] args) {
		
		try {
			   int status = ToolRunner.run(new moviegenrerating(), args);
			   System.exit(status);
			} catch (Exception e) {
			   e.printStackTrace();
			  }
	 }

	 public int run(String[] args) throws Exception {
		 
		  if (args.length != 2) {
		   System.err.printf("Usage: %s <userdictionary> <input1> <output>\n", getClass().getSimpleName());
		   ToolRunner.printGenericCommandUsage(System.err);
		   return -1;
		  }
		  Job job = new Job();
		  job.setJarByClass(getClass());
		  job.setJobName("MapReduceMovie");
		  // input path
		  FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		  // output path
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		  job.setMapperClass(JoinMapper.class);
		  job.setReducerClass(JoinReducer.class);
		
		  job.setMapOutputKeyClass(Text.class);
		  job.setMapOutputValueClass(IntWritable.class);
		
		  job.addCacheFile(new URI("hdfs://localhost:9000/input/user.log"));
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(DoubleWritable.class);
		
		  return job.waitForCompletion(true) ? 0 : 1;
	}
	

}
