
public class ConsecutiveEventTracker {
	String eventType;
	String startDate;
	String lastOccurance;
	Integer counter;
	
	public ConsecutiveEventTracker(String _eventType) {
		eventType = _eventType;
		startDate = "";
		lastOccurance = "";
		counter = 0;
	}
	
	public void increment(String eventDate)
	{
		if (startDate.equals(""))
		{
			startDate = eventDate;
		}
		
		if (!eventDate.equals(lastOccurance))
		{
			lastOccurance = eventDate;
			counter++;
		}
	}
	
	public void reset()
	{
		startDate = "";
		lastOccurance = "";
		counter = 0;		
	}
	
	public String getEventDateRange()
	{
		String startDateOutput = startDate.substring(0, 8);
		String endDateOutput = lastOccurance.substring(0, 8);
/*  Not sure why, but when this code is active, it seems to mess up the sort order, leading to bad output (disorganized)		
		if (startDate.endsWith("1"))
		{
			startDateOutput = startDateOutput + "Game1";
		}
		else if (startDate.endsWith("2"))
		{
			startDateOutput = startDateOutput + "Game2";
		}
		
		if (lastOccurance.endsWith("1"))
		{
			endDateOutput = endDateOutput + "Game1";
		}
		else if (lastOccurance.endsWith("2"))
		{
			endDateOutput = endDateOutput + "Game2";
		}
*/
		return startDateOutput + "-" + endDateOutput;
	}
}
