package mp2.aviation;

public class FlightInfo {
	final String origin;
	final String dest;
	final Integer flightNumber;
	final Integer date;
	final Integer departTime;
	final Integer arrivalTime;
	final Integer arrivalDelay;

	public FlightInfo(String[] values) {
		origin = values[Util.ORIGIN_INDEX];
		dest = values[Util.DEST_INDEX];
		flightNumber = Integer.parseInt(values[Util.FLIGHT_NUM_INDEX]);
		Integer year = Integer.parseInt(values[Util.YEAR_INDEX]);
		Integer month = Integer.parseInt(values[Util.MONTH_INDEX]);
		Integer day = Integer.parseInt(values[Util.DAY_INDEX]);
		date = year * 10000 + month * 100 + day;
		departTime = Integer.parseInt(values[Util.CRC_DEPART_TIME_INDEX]);
		arrivalTime = Integer.parseInt(values[Util.CRC_ARRIVE_TIME_INDEX]);
		int i = values[Util.ARR_DELAY_INDEX].indexOf(".");
		String arrivalDelayStr = i >= 0 ? values[Util.ARR_DELAY_INDEX].substring(0, i) : values[Util.ARR_DELAY_INDEX];
		int temp = 0;
		try {
			temp = Integer.parseInt(arrivalDelayStr);
		} catch (NumberFormatException nfe) {
			// just ignore
		}
		arrivalDelay = temp;
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

}

