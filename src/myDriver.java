import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class myDriver {

	public static void main(String[] args) throws Exception {

		/*
		 * Validate that two arguments were passed from the command line.
		 */
		if (args.length != 2) {
			System.out.printf("Usage: myDriver <input dir> <output dir>\n");
			System.exit(-1);
		}

		String intermediatePath = "tempData";
		boolean parseDataResult = parseRetrosheetData(args[0], intermediatePath);

		if (parseDataResult != true)
		{
			System.exit(1);
		}

		boolean sortResults = sortOutputData(intermediatePath, args[1]);

		System.exit(sortResults ? 0 : 2);
	}

	private static boolean sortOutputData(String inputPath, String outputPath) throws Exception {

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
		job.setJobName("My Map-Reduce Job");

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
		job.setReducerClass(SortReducer.class);

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
		job.setJobName("My Map-Reduce Job");

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
		//job.setPartitionerClass(NYSEPartitioner.class);    
		//job.setNumReduceTasks(26);
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
		job.setOutputValueClass(LongWritable.class);    

		/*
		 * Start the MapReduce job and wait for it to finish.
		 * If it finishes successfully, return 0. If not, return 1.
		 */
		return job.waitForCompletion(true);
	}

}

