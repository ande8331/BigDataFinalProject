import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class myReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

	FloatWritable maximum = new FloatWritable();
	
  @Override
  public void reduce(Text key, Iterable<FloatWritable> values, Context context)
      throws IOException, InterruptedException {

	  	maximum.set(-1.0f);
		/*
		 * For each value in the set of values passed to us by the mapper:
		 */
		for (FloatWritable value : values) 
		{
			float temp = value.get();
			if (temp > maximum.get())
			{
				maximum.set(temp);
			}
		}
		context.write(key, maximum);
  }
}