package mp2.aviation;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import au.com.bytecode.opencsv.CSVReader;

public class Util {

	public static final int YEAR_INDEX = 'A' - 'A';
	public static final int MONTH_INDEX = 'C' - 'A';
	public static final int DAY_INDEX = 'D' - 'A';
	public static final int DATE_INDEX = 'F' - 'A';
	public static final int AIRLINE_ID_INDEX = 'H' - 'A';
	public static final int FLIGHT_NUM_INDEX = 'K' - 'A';
	public static final int ORIGIN_INDEX = 'L' - 'A';
	public static final int DEST_INDEX = 'R' - 'A';
	public static final int CRC_DEPART_TIME_INDEX = 'X' - 'A';
	public static final int DEPART_TIME_INDEX = 'Y' - 'A';
	public static final int DEP_DELAY_15_INDEX = 'Z' - 'A' + 1 + 'B' - 'A';
	public static final int CRC_ARRIVE_TIME_INDEX = 'Z' - 'A' + 1 + 'I' - 'A';
	public static final int ARRIVE_TIME_INDEX = 'Z' - 'A' + 1 + 'J' - 'A';
	public static final int ARR_DELAY_INDEX = 'Z' - 'A' + 1 + 'K' - 'A';
	public static final int ARR_DELAY_15_INDEX = 'Z' - 'A' + 1 + 'M' - 'A';
	public static final int CANCELLED_INDEX = 'Z' - 'A' + 1 + 'P' - 'A';

	public static Map<String, String> loadAirportNames(Configuration conf) throws IOException {
		Map<String, String> airportIdToNameMap = new HashMap<>();
        FileSystem fileSystem = FileSystem.get(conf);
        FSDataInputStream fdis = fileSystem.open(new Path(conf.get("airport-names", "/transp-data/aviation/airport-names.csv")));
        InputStreamReader isr = new InputStreamReader(fdis);
    	CSVReader reader = new CSVReader(isr);
    	String[] values = reader.readNext();
    	while (values != null) {
    		airportIdToNameMap.put(values[0], values[1]);
    		values = reader.readNext();
    	}
    	reader.close();
    	return airportIdToNameMap;
	}
	
	public static Map<String, String> loadAirlineNames(Configuration conf) throws IOException {
		Map<String, String> airlineIdToNameMap = new HashMap<>();
    	FileSystem fileSystem = FileSystem.get(conf);
        FSDataInputStream fdis = fileSystem.open(new Path(conf.get("carrier-decode", "/transp-data/aviation/carrier-decode.csv")));
        InputStreamReader isr = new InputStreamReader(fdis);
    	CSVReader reader = new CSVReader(isr);
    	String[] values = reader.readNext();
    	while (values != null) {
    		airlineIdToNameMap.put(values[0], values[3]);
    		values = reader.readNext();
    	}
    	reader.close();
		return airlineIdToNameMap;
	}

    public static String[] FIELDS = {
    	"Year", 
    	"Quarter", 
    	"Month", 
    	"DayofMonth", 
    	"DayOfWeek", 
    	"FlightDate", 
    	"UniqueCarrier", 
    	"AirlineID", 
    	"Carrier", 
    	"TailNum", 
    	"FlightNum", 
    	"Origin", 
    	"OriginCityName", 
    	"OriginState", 
    	"OriginStateFips", 
    	"OriginStateName", 
    	"OriginWac", 
    	"Dest", 
    	"DestCityName", 
    	"DestState", 
    	"DestStateFips", 
    	"DestStateName", 
    	"DestWac", 
    	"CRSDepTime", 
    	"DepTime", 
    	"DepDelay", 
    	"DepDelayMinutes", 
    	"DepDel15", 
    	"DepartureDelayGroups", 
    	"DepTimeBlk", 
    	"TaxiOut", 
    	"WheelsOff", 
    	"WheelsOn", 
    	"TaxiIn", 
    	"CRSArrTime", 
    	"ArrTime", 
    	"ArrDelay", 
    	"ArrDelayMinutes", 
    	"ArrDel15", 
    	"ArrivalDelayGroups", 
    	"ArrTimeBlk", 
    	"Cancelled", 
    	"CancellationCode", 
    	"Diverted", 
    	"CRSElapsedTime", 
    	"ActualElapsedTime", 
    	"AirTime", 
    	"Flights", 
    	"Distance", 
    	"DistanceGroup", 
    	"CarrierDelay", 
    	"WeatherDelay", 
    	"NASDelay", 
    	"SecurityDelay", 
    	"LateAircraftDelay", 
    };
    
    public static int fieldIndex(String fieldName) {
    	int result = -1;
    	for (int i = 0; i < FIELDS.length; i++) {
    		if (FIELDS[i].equals(fieldName)) {
    			result = i;
    			break;
    		}
    	}
    	return result;
    }

    
}
