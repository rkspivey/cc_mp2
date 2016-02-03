package mp2.aviation;

public class FlightInfo implements Comparable<FlightInfo> {
	final String origin;
	final String dest;
	final Integer flightNumber;
	final Integer date;
	final Integer departTime;
	final Integer arrivalTime;
	final Integer arrivalDelay;
	final boolean cancelled;

	public FlightInfo(String[] values) {
		origin = values[Util.ORIGIN_INDEX];
		dest = values[Util.DEST_INDEX];
		flightNumber = Integer.parseInt(values[Util.FLIGHT_NUM_INDEX]);
		Integer year = Integer.parseInt(values[Util.YEAR_INDEX]);
		Integer month = Integer.parseInt(values[Util.MONTH_INDEX]);
		Integer day = Integer.parseInt(values[Util.DAY_INDEX]);
		date = year * 10000 + month * 100 + day;
		int i = values[Util.ARR_DELAY_INDEX].indexOf(".");
		String arrivalDelayStr = i >= 0 ? values[Util.ARR_DELAY_INDEX].substring(0, i) : values[Util.ARR_DELAY_INDEX];
		int temp = Integer.MAX_VALUE;
		try {
			temp = Integer.parseInt(arrivalDelayStr);
		} catch (NumberFormatException nfe) {
			// just ignore
		}
		arrivalDelay = temp;
    	double cancelledField = values != null && values[Util.CANCELLED_INDEX] != null ? Double.parseDouble(values[Util.CANCELLED_INDEX]) : 1;
		
		cancelled = temp == Integer.MAX_VALUE || cancelledField != 0d;
		if (!cancelled) {
			departTime = Integer.parseInt(values[Util.DEPART_TIME_INDEX]);
			arrivalTime = Integer.parseInt(values[Util.ARRIVE_TIME_INDEX]);
		} else {
			departTime = Integer.MAX_VALUE;
			arrivalTime = Integer.MAX_VALUE;			
		}
	}

	public String getOrigin() {
		return origin;
	}

	public String getDest() {
		return dest;
	}

	public Integer getFlightNumber() {
		return flightNumber;
	}

	public Integer getDate() {
		return date;
	}

	public Integer getDepartTime() {
		return departTime;
	}

	public Integer getArrivalTime() {
		return arrivalTime;
	}

	public Integer getArrivalDelay() {
		return arrivalDelay;
	}
	
	public boolean isCancelled() {
		return cancelled;
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append(origin)
			.append(' ')
			.append(dest)
			.append(' ')
			.append(flightNumber)
			.append(' ')
			.append(date)
			.append(' ')
			.append(departTime)
			.append(' ')
			.append(arrivalTime);
		return builder.toString();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((arrivalTime == null) ? 0 : arrivalTime.hashCode());
		result = prime * result + ((date == null) ? 0 : date.hashCode());
		result = prime * result
				+ ((departTime == null) ? 0 : departTime.hashCode());
		result = prime * result + ((dest == null) ? 0 : dest.hashCode());
		result = prime * result
				+ ((flightNumber == null) ? 0 : flightNumber.hashCode());
		result = prime * result + ((origin == null) ? 0 : origin.hashCode());
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
		FlightInfo other = (FlightInfo) obj;
		if (arrivalTime == null) {
			if (other.arrivalTime != null)
				return false;
		} else if (!arrivalTime.equals(other.arrivalTime))
			return false;
		if (date == null) {
			if (other.date != null)
				return false;
		} else if (!date.equals(other.date))
			return false;
		if (departTime == null) {
			if (other.departTime != null)
				return false;
		} else if (!departTime.equals(other.departTime))
			return false;
		if (dest == null) {
			if (other.dest != null)
				return false;
		} else if (!dest.equals(other.dest))
			return false;
		if (flightNumber == null) {
			if (other.flightNumber != null)
				return false;
		} else if (!flightNumber.equals(other.flightNumber))
			return false;
		if (origin == null) {
			if (other.origin != null)
				return false;
		} else if (!origin.equals(other.origin))
			return false;
		return true;
	}

	@Override
	public int compareTo(FlightInfo o) {
		int result = origin.compareTo(o.origin);
		if (result == 0) {
			result = dest.compareTo(o.dest);
		}
		if (result == 0) {
			result = date.compareTo(o.date);
		}
		if (result == 0) {
			result = flightNumber.compareTo(o.flightNumber);
		}
		if (result == 0) {
			result = departTime.compareTo(o.departTime);
		}
		if (result == 0) {
			result = arrivalTime.compareTo(o.arrivalTime);
		}
		return result;
	}

}

