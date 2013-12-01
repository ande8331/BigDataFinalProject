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
		SameGameWalksFound,
		HitsFound,
		SameGameHitsFound,
		Terminators,
		PlayerFlush
	}
	Text output = new Text();
	String lastGame = "";
	String lastPlayer = "";
	String lastWalk = "";
	String lastHit = "";
	LongWritable walkCount = new LongWritable(0);
	LongWritable hitCount = new LongWritable(0);
	
	String hitStreakStart = "";
	String hitStreakEnd = "";
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
	  		
	  		// Check for breaks in streaks and put them out
	  		if (hitCount.get() > 1)
	  		{
  				output.set(lastPlayer+"-Hits");
	  			context.write(output , hitCount);
	  			hitStreakStart = "";
	  			hitStreakEnd = "";
	  		}

	  		walkCount.set(0);
	  		lastWalk = "";
	  		hitCount.set(0);
	  		lastHit = "";

	  	}
	  	else if (!lastGame.equals(key.gameId))
	  	{
	  		if (!lastGame.equals(lastWalk))
	  		{
		  		// Check for breaks in streaks and put them out
		  		if (walkCount.get() > 1)
		  		{
	  				output.set(lastPlayer+"-Walks");
		  			context.write(output , walkCount);
		  		}
		  		walkCount.set(0);
		  		lastWalk = "";	  			
	  		}
	  		
	  		if (!lastGame.equals(lastHit))
	  		{
		  		// Check for breaks in streaks and put them out
		  		if (hitCount.get() > 1)
		  		{
	  				output.set(lastPlayer+ "-Hits" + " : " + hitStreakStart + "-" + hitStreakEnd);
		  			context.write(output , hitCount);
		  		}
		  		hitCount.set(0);
		  		lastHit = "";	  			
	  			hitStreakStart = "";
	  			hitStreakEnd = "";
	  		}
	  	}
	  	
	  	lastGame = key.gameId;
	  	lastPlayer = key.playerId;	 
		  
		//for (Text value : values)
	  	for (Text value: values)
		{
			//output.set(output.toString() + "," + value.toString());
	  		//output.set(output.get() + value.get());
	  		
	  		if (value.toString().equals("Walk"))
	  		{
	  			context.getCounter(ReducerErrorCounters.WalksFound).increment(1);
	  			
	  			if (!lastWalk.equals(lastGame))
	  			{
		  			lastWalk = lastGame;
		  			walkCount.set(walkCount.get() + 1);
	  			}
	  			else
	  			{
	  				context.getCounter(ReducerErrorCounters.SameGameWalksFound).increment(1);
	  			}
	  		}
	  		
	  		if (value.toString().equals("Hit"))
	  		{
	  			context.getCounter(ReducerErrorCounters.HitsFound).increment(1);
	  			
	  			if (hitStreakStart.length() < 1)
	  			{
	  				hitStreakStart = lastGame;
	  			}
	  			hitStreakEnd = lastGame;
	  			
	  			if (!lastHit.equals(lastGame))
	  			{
		  			lastHit = lastGame;
		  			hitCount.set(hitCount.get() + 1);
	  			}
	  			else
	  			{
	  				context.getCounter(ReducerErrorCounters.SameGameHitsFound).increment(1);
	  			}
	  		}
		}
  }
}