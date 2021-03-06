import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;

public class TextComparatorInverted extends WritableComparator {

	public TextComparatorInverted() { 
		super(Text.class);
	}

	@Override
	public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

		// This happens to work right now, but if the strings being compared were of different length, it wouldn't work...		
		return Text.Comparator.compareBytes(b1, s1, l1, b2, s2, l2) * -1;
	}
}
