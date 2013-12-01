import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


//public class gameEventWritable implements Writable {
public class gameEventWritable implements WritableComparable<gameEventWritable> {
	String ballpark;
	String gameId;
	String playerId;
	String inning;
	String score;
	boolean endRecordMarker;


	public gameEventWritable() {}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(gameId);
		out.writeUTF(playerId);
		out.writeUTF(inning);
		out.writeUTF(score);
		out.writeBoolean(endRecordMarker);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		gameId = in.readUTF();
		playerId = in.readUTF();
		inning = in.readUTF();
		score = in.readUTF();
		endRecordMarker = in.readBoolean();
	}

	@Override
	public int compareTo(gameEventWritable o) {
		
    	int result = this.playerId.compareTo(o.playerId);
    	if (result == 0)
    	{
    		result = this.gameId.compareTo(o.gameId);
    		
    		if (result == 0)
    		{
    			result = this.inning.compareTo(o.inning);
    			
    			if (result ==0)
    			{
    				result = this.score.compareTo(o.score);
    			}
    		}
    	}    	
        return result;
	}

}

