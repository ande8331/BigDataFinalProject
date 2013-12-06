import java.nio.ByteBuffer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

public class TextComparatorInverted extends WritableComparator {
	
	public TextComparatorInverted() { 
		super(Text.class);
	}
	
	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
		int n1 = WritableUtils.decodeVIntSize(b1[s1]);
		int n2 = WritableUtils.decodeVIntSize(b2[s2]);
		return compareBytes(b1, s1+n1, l1-n1, b2, s2+n2, l2-n2) * -1;
      }
}
