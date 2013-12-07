import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class myDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		String intermediatePath = "tempData";
		Configuration conf = new Configuration();		
		
		FileSystem fs = FileSystem.get(conf);
		
		// Delete it here in case a previous failure didn't remove it.
		fs.delete(new Path(intermediatePath), true);
		
		boolean parseDataResult = parseRetrosheetData(args[0], intermediatePath);

		if (parseDataResult != true)
		{
			System.exit(1);
		}

		boolean sortResults = sortOutputData(intermediatePath, args[1]);

		// Delete it now to free up space
		fs.delete(new Path(intermediatePath), true);

		
		return sortResults ? 0 : 2;
	}
	
	public static void main(String[] args) throws Exception {

		/*
		 * Validate that two arguments were passed from the command line.
		 */
		//if (args.length != 2) {
		//	System.out.printf("Usage: myDriver <input dir> <output dir>\n");
		//	System.exit(-1);
		//}

		int exitCode = ToolRunner.run(new myDriver(), args);
		System.exit(exitCode);
	}

	private static boolean sortOutputData(String inputPath, String outputPath) throws Exception {

		/*
		 * Instantiate a Job object for your job's configuration. 
		 */
		Job job = new Job();		
		DistributedCache.addCacheFile(new Path("_roster.txt").toUri(), job.getConfiguration());
		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running 
		 * mapper and reducer tasks.
		 */
		job.setJarByClass(myDriver.class);

		/*
		 * Specify an easily-decipherable name for the job.
		 * This job name will appear in reports and logs.
		 */
		job.setJobName("Sort Streaks Job");

		/*
		 * Specify the paths to the input and output data based on the
		 * command-line arguments.
		 */
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		/*
		 * Specify the mapper and reducer classes.
		 */
		job.setMapperClass(SortMapper.class);
		job.setSortComparatorClass(TextComparatorInverted.class);
		job.setPartitionerClass(SortPartitioner.class);
		job.setReducerClass(SortReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		
		/*
		 * Specify Map Output key and value classes.
		 */
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);	
		
		/*
		 * Specify the job's output key and value classes.
		 */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);    

		/*
		 * Start the MapReduce job and wait for it to finish.
		 * If it finishes successfully, return 0. If not, return 1.
		 */
		
		System.err.println(job.getConfiguration().get("mapred.cache.files"));
		
		return job.waitForCompletion(true);
	}

	private static boolean parseRetrosheetData(String inputPath, String outputPath) throws Exception {

		/*
		 * Instantiate a Job object for your job's configuration. 
		 */
		Job job = new Job();

		/*
		 * Specify the jar file that contains your driver, mapper, and reducer.
		 * Hadoop will transfer this jar file to nodes in your cluster running 
		 * mapper and reducer tasks.
		 */
		job.setJarByClass(myDriver.class);

		/*
		 * Specify an easily-decipherable name for the job.
		 * This job name will appear in reports and logs.
		 */
		job.setJobName("Parse Data Files Job");

		/*
		 * Specify the paths to the input and output data based on the
		 * command-line arguments.
		 */
		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		/*
		 * Specify the mapper and reducer classes.
		 */
		job.setMapperClass(myMapper.class);
		job.setPartitionerClass(gameEventPartitioner.class);    
		job.setReducerClass(myReducer.class);

		/*
		 * Specify Map Output key and value classes.
		 */
		job.setMapOutputKeyClass(gameEventWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		/*
		 * Specify the job's output key and value classes.
		 */
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);    

		/*
		 * Start the MapReduce job and wait for it to finish.
		 * If it finishes successfully, return 0. If not, return 1.
		 */
		return job.waitForCompletion(true);
	}

}

