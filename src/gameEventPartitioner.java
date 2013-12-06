import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class gameEventPartitioner<K2, V2> extends Partitioner<gameEventWritable, Text> {

	public int getPartition(gameEventWritable key, Text value, int numReduceTasks) 
	{
		return (key.playerId.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
}