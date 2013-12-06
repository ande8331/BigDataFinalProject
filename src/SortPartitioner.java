import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class SortPartitioner<K2, V2> extends Partitioner<Text, NullWritable> {

	public int getPartition(Text key, NullWritable value, int numReduceTasks) 
	{
		String tempKey = key.toString();
		String[] tokens = tempKey.split(":");
		return (tokens[0].hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	}
}