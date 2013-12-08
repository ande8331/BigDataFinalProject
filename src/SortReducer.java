import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
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
public class SortReducer extends Reducer<Text, NullWritable, Text, Text> {

  /*
   * The reduce method runs once for each key received from
   * the shuffle and sort phase of the MapReduce framework.
   * The method receives a key of type Text, a set of values of type
   * IntWritable, and a Context object.
   */
	
	enum ReducerErrorCounters {
		validPlayerTokenCount,
		invalidTokenCount,
		invalidPlayerTokenCount,
		playerLookupFailed
	}

  Map<String, String> playerLookupMap = new HashMap<String, String>();
	
  @Override
  public void setup(Context context) throws IOException, InterruptedException 
  {	  
	  Path[] cacheFiles = context.getLocalCacheFiles();
	  System.err.println("Number of cache files: " + cacheFiles.length);
	  System.err.println("Cache File 0: " + cacheFiles[0]);
	  System.err.println("Cache File 0: " + cacheFiles[0].toString());
	  System.err.println("Cache File 0: " + cacheFiles[0].toUri());
	  //FileInputStream fs = new FileInputStream(cacheFiles[0].toString());
	  
	  File file = new File(cacheFiles[0].toString());
	  BufferedReader reader = new BufferedReader(new FileReader(file));	  
	  String text = null;
	  
	  while ((text = reader.readLine()) != null)
	  {
		  String[] tokens = text.split(",");
		  if (tokens.length == 4)
		  {
			  playerLookupMap.put(tokens[2], tokens[1] + " " + tokens[0]);
			  context.getCounter(ReducerErrorCounters.validPlayerTokenCount).increment(1);
		  }
		  else
		  {
			  context.getCounter(ReducerErrorCounters.invalidPlayerTokenCount).increment(1);
		  }
	  }
	  reader.close();
	  
  }
	
  String lastKey = "";
  String lastStreakValue = "";
  int lastKeyCounter = 0;
  Text keyOutput = new Text();
  Text valueOutput = new Text();
  @Override
	public void reduce(Text key, Iterable<NullWritable> values, Context context)
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
	    
	    if (tokens.length == 4)
	    {	  
  
			  if (!lastKey.equals(tokens[0]))
			  {
				  lastKeyCounter = 0;
				  lastKey = tokens[0];
			  }
		
			  if ((lastKeyCounter < 5) || (tokens[1].equals(lastStreakValue)))
			  {
					//for (Text value: values)
					{
						//valueOutput.set(value);
						lastStreakValue = tokens[1];
						keyOutput.set(tokens[0]);
						
						String playerName = playerLookupMap.get(tokens[3]);
						if (playerName == null)
						{
							playerName = tokens[3];
							System.err.println("Lookup failed for: " + playerName + ".");
							context.getCounter(ReducerErrorCounters.playerLookupFailed).increment(1);
						}
						
						valueOutput.set(tokens[1] + " : " + tokens[2] + ":" + playerName);
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