import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class myReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	//FloatWritable maximum = new FloatWritable();

	//Text output = new Text();
	LongWritable output = new LongWritable();
  @Override
  public void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {

		/*
		 * For each value in the set of values passed to us by the mapper:
		 */
	  
	  	//output.set("");
	  	output.set(0);
	  
		//for (Text value : values)
	  	for (LongWritable value: values)
		{
			//output.set(output.toString() + "," + value.toString());
	  		output.set(output.get() + value.get());
		}
		context.write(key, output);
  }
}