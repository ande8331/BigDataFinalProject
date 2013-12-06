import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/* 
 * To define a reduce function for your MapReduce job, subclass 
 * the Reducer class and override the reduce method.
 * The class definition requires four parameters: 
 *   The data type of the input key (which is the output key type 
 *   from the mapper)
 *   The data type of the input value (which is the output value 
 *   type from the mapper)
 *   The data type of the output key
 *   The data type of the output value
 */   
public class SortReducer extends Reducer<Text, Text, Text, Text> {

  /*
   * The reduce method runs once for each key received from
   * the shuffle and sort phase of the MapReduce framework.
   * The method receives a key of type Text, a set of values of type
   * IntWritable, and a Context object.
   */
	
	enum ReducerErrorCounters {
		invalidTokenCount
	}

  String lastKey = "";
  int lastKeyCounter = 0;
  Text keyOutput = new Text();
  Text valueOutput = new Text();
  @Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

	  
	    String line = key.toString();

	    /*
	     * The line.split("\\W+") call uses regular expressions to split the
	     * line up by non-word characters.
	     * 
	     * If you are not familiar with the use of regular expressions in
	     * Java code, search the web for "Java Regex Tutorial." 
	     */
	    String[] tokens = line.split(":");
	    
	    if (tokens.length == 2)
	    {	  
  
			  if (!lastKey.equals(tokens[0]))
			  {
				  lastKeyCounter = 0;
				  lastKey = tokens[0];
			  }
		
			  if (lastKeyCounter < 20)
			  {
					for (Text value: values)
					{
						//valueOutput.set(value);
						keyOutput.set(tokens[0]);
						valueOutput.set(tokens[1] + " : " + value.toString());
					    context.write(keyOutput, valueOutput);
					    lastKeyCounter++;
					}
			  }		
	    }
	    else
	    {
	    	context.getCounter(ReducerErrorCounters.invalidTokenCount).increment(1);	
	    }

  }
}