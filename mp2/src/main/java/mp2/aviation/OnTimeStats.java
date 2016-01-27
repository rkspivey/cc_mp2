package mp2.aviation;

public class OnTimeStats implements java.io.Serializable, Comparable<OnTimeStats> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6377998416291290446L;

	final int onTimeCount;
	final int totalCount;
	final String id;
	
	public OnTimeStats(String id, int onTimeCount, int totalCount) {
		this.onTimeCount = onTimeCount;
		this.totalCount = totalCount;
		this.id = id;
	}

	public OnTimeStats(String id, String text) {
		this.id = id;
		String[] values = text.split(" ");
		this.onTimeCount = Integer.parseInt(values[1]);
		this.totalCount = Integer.parseInt(values[2]);
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(getOnTimePercentage());
		builder.append(' ');
		builder.append(getOnTimeCount());
		builder.append(' ');
		builder.append(getTotalCount());
		return builder.toString();
	}

	public int getOnTimeCount() {
		return onTimeCount;
	}

	public int getTotalCount() {
		return totalCount;
	}

	public String getId() {
		return id;
	}

	public double getOnTimePercentage() {
		return (double) onTimeCount / (double) totalCount;
	}
	
	@Override
	public int compareTo(OnTimeStats o) {
		Double percentage = o.getOnTimePercentage();
		return percentage.compareTo(getOnTimePercentage()); 
	}
	
}
