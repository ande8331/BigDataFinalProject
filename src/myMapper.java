import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import au.com.bytecode.opencsv.CSVParser;


public class myMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	Text outputKey = new Text();
	//Text outputValue = new Text();
	LongWritable outputValue = new LongWritable(1);
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
			  outputKey.set(tokens[10]);
			  //outputValue.set(1);
			  context.write(outputKey, outputValue);
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
