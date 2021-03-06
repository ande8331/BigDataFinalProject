import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import au.com.bytecode.opencsv.CSVParser;


public class EventMapper extends Mapper<LongWritable, Text, gameEventWritable, Text> {

	gameEventWritable outputKey = new gameEventWritable();
	Text outputValue = new Text();
	CSVParser myCSVParser = new CSVParser(',','\"');

	enum MapperErrorCounters {
		EmptyLine,
		InvalidNumberOfTokens,
		PartialAtBat
	}

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String temp = value.toString();

		String[] tokens = myCSVParser.parseLine(temp);
		if (tokens.length == 97)
		{
			// Count how many plate appearances per player

			// Throw out anything with a "F" in the batter event flag as this indicates
			// either steal, pickoff or balk occured.
			if (tokens[35].equals("F"))
			{
				context.getCounter(MapperErrorCounters.PartialAtBat).increment(1);  
			}
			else
			{
				int eventType = Integer.valueOf(tokens[34]);

				outputKey.ballpark = tokens[0].substring(0,  3);
				outputKey.gameId = tokens[0].substring(3);
				outputKey.playerId = tokens[10];
				outputKey.inning = tokens[2];
				outputKey.score = tokens[8] + "-" + tokens[9];
				outputKey.endRecordMarker = false;

				if (eventType == 3)
				{
					outputValue.set("Strikeout");
					context.write(outputKey, outputValue);

					if (tokens[7].endsWith("C"))
					{
						outputValue.set("Strikeout - Caught Looking");
						context.write(outputKey, outputValue);					  
					}
				}
				else if (eventType == 14)
				{
					outputValue.set("Walk");
					context.write(outputKey, outputValue);
					outputValue.set("Reached Base");
					context.write(outputKey, outputValue);
				}
				else if (eventType == 15)
				{
					outputValue.set("Intentional Walk");
					context.write(outputKey, outputValue);
					outputValue.set("Walk");
					context.write(outputKey, outputValue);
					outputValue.set("Reached Base");
					context.write(outputKey, outputValue);
				}
				else if (eventType == 16)
				{
					outputValue.set("Hit By Pitch");
					context.write(outputKey, outputValue);
					outputValue.set("Reached Base");
					context.write(outputKey, outputValue);
				}
				else if (eventType == 20)
				{
					outputValue.set("Single");
					context.write(outputKey, outputValue);
					outputValue.set("Hit");
					context.write(outputKey, outputValue);
					outputValue.set("Reached Base");
					context.write(outputKey, outputValue);
				}
				else if (eventType == 21)
				{
					outputValue.set("Double");
					context.write(outputKey, outputValue);
					outputValue.set("Hit");
					context.write(outputKey, outputValue);
					outputValue.set("Extra Base Hit");
					context.write(outputKey, outputValue);
					outputValue.set("Reached Base");
					context.write(outputKey, outputValue);
				}
				else if (eventType == 22)
				{
					outputValue.set("Triple");
					context.write(outputKey, outputValue);
					outputValue.set("Hit");
					context.write(outputKey, outputValue);
					outputValue.set("Extra Base Hit");
					context.write(outputKey, outputValue);
					outputValue.set("Reached Base");
					context.write(outputKey, outputValue);
				}
				else if (eventType == 23)
				{
					outputValue.set("Home Run");
					context.write(outputKey, outputValue);
					outputValue.set("Hit");
					context.write(outputKey, outputValue);
					outputValue.set("Extra Base Hit");
					context.write(outputKey, outputValue);
					outputValue.set("Reached Base");
					context.write(outputKey, outputValue);
				}
				else
				{
					// If none of the events above, note it to break the streak
					outputValue.set("");
					outputKey.endRecordMarker = true;
					context.write(outputKey, outputValue);				  
				}

				if (tokens[31].equals("T"))
				{
					outputValue.set("Pinch Hitter");
					context.write(outputKey, outputValue);
				}

				if (tokens[41].equals("T"))
				{
					outputValue.set("Hit Into Double Play");
					context.write(outputKey, outputValue);
				}

				if (tokens[42].equals("T"))
				{
					outputValue.set("Hit Into Double Play");
					context.write(outputKey, outputValue);
					outputValue.set("Hit Into Triple Play");
					context.write(outputKey, outputValue);				  
				}

				if (tokens[48].equals("T"))
				{
					outputValue.set("Bunt");
					context.write(outputKey, outputValue);
				}

				int RBICount = Integer.parseInt(tokens[43].toString());
				if (RBICount > 0)
				{
					outputValue.set("RBI");
					context.write(outputKey, outputValue);				  				  
				}
			}		  
		}
		else if (tokens.length == 0)
		{
			context.getCounter(MapperErrorCounters.EmptyLine).increment(1);
		}
		else
		{
			context.getCounter(MapperErrorCounters.InvalidNumberOfTokens).increment(1);
		}
	}
}
