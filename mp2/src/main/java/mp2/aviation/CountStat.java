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
		return Integer.valueOf(totalCount).compareTo(o.totalCount);
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
		CountStat other = (CountStat) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}

}
