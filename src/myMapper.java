import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import au.com.bytecode.opencsv.CSVParser;


public class myMapper extends Mapper<LongWritable, Text, gameEventWritable, Text> {

	gameEventWritable outputKey = new gameEventWritable();
	//Text outputKey = new Text();
	//Text outputValue = new Text();
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
	  
	  //String[] tokens = temp.split(",");
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
			  
			  // Set key as Batter Name - Game - Inning - Visitor Score - Home Score (Visitor/Home score to handle through the lineup situations)
			  //outputKey.set(tokens[10] + "-" + tokens[0] + "-" + tokens[2] + "-" + tokens[8] + "-" + tokens[9]);
			  outputKey.gameId = tokens[0];
			  outputKey.playerId = tokens[10];
			  outputKey.inning = tokens[2];
			  outputKey.score = tokens[8] + "-" + tokens[9];
			  outputKey.endRecordMarker = false;
			  
			  if (eventType == 3)
			  {
				  outputValue.set("Strikeout");
				  context.write(outputKey, outputValue);
			  }
			  else if (eventType == 14)
			  {
				  outputValue.set("Walk");
				  context.write(outputKey, outputValue);
			  }
			  else if (eventType == 15)
			  {
				  outputValue.set("Intentional Walk");
				  context.write(outputKey, outputValue);
				  outputValue.set("Walk");
				  context.write(outputKey, outputValue);
			  }
			  else if (eventType == 16)
			  {
				  outputValue.set("HBP");
				  context.write(outputKey, outputValue);
			  }
			  else if (eventType == 20)
			  {
				  outputValue.set("Single");
				  context.write(outputKey, outputValue);
				  outputValue.set("Hit");
				  context.write(outputKey, outputValue);
			  }
			  else if (eventType == 21)
			  {
				  outputValue.set("Double");
				  context.write(outputKey, outputValue);
				  outputValue.set("Hit");
				  context.write(outputKey, outputValue);
			  }
			  else if (eventType == 22)
			  {
				  outputValue.set("Triple");
				  context.write(outputKey, outputValue);
				  outputValue.set("Hit");
				  context.write(outputKey, outputValue);
			  }
			  else if (eventType == 23)
			  {
				  outputValue.set("Home Run");
				  context.write(outputKey, outputValue);
				  outputValue.set("Hit");
				  context.write(outputKey, outputValue);
			  }
			  else
			  {
				  // If none of the events above, note it to break the streak
				  outputValue.set("");
				  outputKey.endRecordMarker = true;
				  context.write(outputKey, outputValue);				  
			  }
			  
			  //outputKey.set(tokens[10]);
			  //outputValue.set(1);
			  //context.write(outputKey, outputValue);
		  }		  
	  }
	  else if (tokens.length ==0)
	  {
		  context.getCounter(MapperErrorCounters.EmptyLine).increment(1);
	  }
	  else
	  {
		  context.getCounter(MapperErrorCounters.InvalidNumberOfTokens).increment(1);
	  }
  }
}
