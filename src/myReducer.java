import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class myReducer extends Reducer<gameEventWritable, Text, Text, LongWritable> {

	//FloatWritable maximum = new FloatWritable();
	enum ReducerErrorCounters {
		WalksFound,
		Terminators,
		PlayerFlush
	}
	Text output = new Text();
	String lastGame = "";
	String lastPlayer = "";
	String lastWalk = "";
	LongWritable walkCount = new LongWritable(0);
	//LongWritable output = new LongWritable();
  @Override
  public void reduce(gameEventWritable key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {

		/*
		 * For each value in the set of values passed to us by the mapper:
		 */
	  
	  	if (!lastPlayer.equals(key.playerId))
	  	{
	  		context.getCounter(ReducerErrorCounters.PlayerFlush).increment(1);
	  		
	  		// Check for breaks in streaks and put them out
	  		if (walkCount.get() > 1)
	  		{
  				output.set(lastPlayer+"-Walks");
	  			context.write(output , walkCount);
	  		}
	  		walkCount.set(0);
	  		lastWalk = "";
	  	}
	  	lastGame = key.gameId;
	  	lastPlayer = key.playerId;	 
		  
		//for (Text value : values)
	  	for (Text value: values)
		{
			//output.set(output.toString() + "," + value.toString());
	  		//output.set(output.get() + value.get());
	  		
	  		if ((value.toString().equals("Walk")) && (!lastWalk.equals(lastGame)))
	  		{
	  			lastWalk = lastGame;
	  			walkCount.set(walkCount.get() + 1);
	  			context.getCounter(ReducerErrorCounters.WalksFound).increment(1);
	  		}
		}
  }
}