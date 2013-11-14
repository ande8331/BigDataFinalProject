import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class myMapper extends Mapper<LongWritable, Text, Text, Text> {

	Text outputKey = new Text();
	Text outputValue = new Text();
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

	  String temp = value.toString();
	  
	  String[] tokens = temp.split(",");
	  if (tokens.length == 9)
	  {
		  outputKey.set(key.toString());
		  outputValue.set(tokens[1]);
		  context.write(outputKey, outputValue);
	  }
	  else
	  {
		  // Fail counter
	  }
  }
}
