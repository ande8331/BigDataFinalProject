import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class myReducer extends Reducer<gameEventWritable, Text, Text, LongWritable> {

	ArrayList<ConsecutiveEventTracker> events = new ArrayList<ConsecutiveEventTracker>();
	private void checkCounters(Context context, String currentGame) throws IOException, InterruptedException
	{
		for (ConsecutiveEventTracker event: events)
		{
			if (!event.lastOccurance.equals(currentGame))
			{
				if (event.counter > 1)
				{
					output.set(lastPlayer + ":" + event.toString());
					outputValue.set(event.counter);
					context.write(output , outputValue);
				}
				event.reset();
			}
		}
		
	}

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
	LongWritable outputValue = new LongWritable(0);
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

	  		checkCounters(context, "");
/*	  		
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
*/
	  	}
	  	else if (!lastGame.equals(key.gameId))
	  	{
	  		checkCounters(context, lastGame);
/*	  		
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
*/
	  	}
	  	
	  	lastGame = key.gameId;
	  	lastPlayer = key.playerId;	 
		  
		//for (Text value : values)
	  	for (Text value: values)
		{
			//output.set(output.toString() + "," + value.toString());
	  		//output.set(output.get() + value.get());
	  		if (value.getLength() > 0)
	  		{
		  		boolean keyFound = false;
		  		
		  		for (ConsecutiveEventTracker e: events)
		  		{
		  			if (value.toString().equals(e.eventType))
		  			{
		  				keyFound = true;
		  				e.increment(lastGame);
		  				break;
		  			}	  			
		  		}
		  		
		  		if (keyFound == false)
		  		{
		  			ConsecutiveEventTracker myTracker = new ConsecutiveEventTracker(value.toString());
		  			myTracker.counter = 1;
		  			myTracker.startDate = lastGame;
		  			myTracker.lastOccurance = lastGame;
		  			events.add(myTracker);
		  		}
	  		}
		}
  }
}