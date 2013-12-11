import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StreakReducer extends Reducer<gameEventWritable, Text, Text, NullWritable> {

	ArrayList<ConsecutiveEventTracker> events = new ArrayList<ConsecutiveEventTracker>();
	private void checkCounters(Context context, String currentGame) throws IOException, InterruptedException
	{
		for (ConsecutiveEventTracker event: events)
		{
			if (!event.lastOccurance.equals(currentGame))
			{
				if (event.counter > 1)
				{
					// Pad the numbers to with 0's to avoid goofy sort issues
					// This could be avoided if the sort mapper took the key in as a composite, or parsed it to int rather than text...
					outputKey.set(event.eventType + ":"+ String.format("%05d", event.counter) + ":" + event.getEventDateRange() + ":" + lastPlayer);
					context.write(outputKey , NullWritable.get());					
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
		PlayerFlush,
		CleanupCalled
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException
	{
		context.getCounter(ReducerErrorCounters.CleanupCalled).increment(1);
		checkCounters(context, "");		
	}

	Text outputKey = new Text();
	String lastGame = "";
	String lastPlayer = "";
	String lastWalk = "";
	String lastHit = "";
	Text outputValue = new Text();
	String hitStreakStart = "";
	String hitStreakEnd = "";
	
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
		}
		else if (!lastGame.equals(key.gameId))
		{
			checkCounters(context, lastGame);
		}

		lastGame = key.gameId;
		lastPlayer = key.playerId;	 

		for (Text value: values)
		{
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