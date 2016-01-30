package mp2.aviation;

public class CountStat implements java.io.Serializable, Comparable<CountStat> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -6377998416291290446L;

	final int totalCount;
	final String id;
	
	public CountStat(String id, int totalCount) {
		this.totalCount = totalCount;
		this.id = id;
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(getTotalCount());
		return builder.toString();
	}

	public int getTotalCount() {
		return totalCount;
	}

	public String getId() {
		return id;
	}

	@Override
	public int compareTo(CountStat o) {
		return Integer.valueOf(o.totalCount).compareTo(totalCount); 
	}
	
}
