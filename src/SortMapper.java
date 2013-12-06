import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/* 
 * To define a map function for your MapReduce job, subclass 
 * the Mapper class and override the map method.
 * The class definition requires four parameters: 
 *   The data type of the input key
 *   The data type of the input value
 *   The data type of the output key (which is the input key type 
 *   for the reducer)
 *   The data type of the output value (which is the input value 
 *   type for the reducer)
 */

public class SortMapper extends Mapper<LongWritable, Text, Text, Text> {

  /*
   * The map method runs once for each line of text in the input file.
   * The method receives a key of type LongWritable, a value of type
   * Text, and a Context object.
   */
	enum MapperErrorCounters {
		invalidTokenCount
	}
	

	Text keyOutput = new Text();
	Text valueOutput = new Text();
	
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
  
    /*
     * Convert the line, which is received as a Text object,
     * to a String object.
     */

    /*
     * The line.split("\\W+") call uses regular expressions to split the
     * line up by non-word characters.
     * 
     * If you are not familiar with the use of regular expressions in
     * Java code, search the web for "Java Regex Tutorial." 
     */
	  
	    String line = value.toString();

	    /*
	     * The line.split("\\W+") call uses regular expressions to split the
	     * line up by non-word characters.
	     * 
	     * If you are not familiar with the use of regular expressions in
	     * Java code, search the web for "Java Regex Tutorial." 
	     */
	    String[] tokens = line.split("\\t");
	    
	    if (tokens.length == 2)
	    {	  
    		keyOutput.set(tokens[0]);
    		valueOutput.set(tokens[1]);
	    	context.write(keyOutput, valueOutput);
	    }
	    else
	    {
	    	context.getCounter(MapperErrorCounters.invalidTokenCount).increment(1);	
	    }
  }
}