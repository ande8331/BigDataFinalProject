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

	public void increment(String eventDate) {
		if (startDate.equals("")) {
			startDate = eventDate;
		}

		if (!eventDate.equals(lastOccurance)) {
			lastOccurance = eventDate;
			counter++;
		}
	}

	public void reset() {
		startDate = "";
		lastOccurance = "";
		counter = 0;
	}

	public String getEventDateRange() {
		String startDateOutput = startDate.substring(0, 8);
		String endDateOutput = lastOccurance.substring(0, 8);

		return startDateOutput + "-" + endDateOutput;
	}
}
