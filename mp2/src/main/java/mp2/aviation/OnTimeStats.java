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
		Double percentage = getOnTimePercentage();
		return percentage.compareTo(o.getOnTimePercentage()); 
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		OnTimeStats other = (OnTimeStats) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}
	
}
