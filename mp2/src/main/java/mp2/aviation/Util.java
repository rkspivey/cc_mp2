package mp2.aviation;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import au.com.bytecode.opencsv.CSVReader;

public class Util {

	public static final int YEAR_INDEX = 0;
	public static final int MONTH_INDEX = 1;
	public static final int DAY_INDEX = 2;
	public static final int DATE_INDEX = 3;
	public static final int AIRLINE_ID_INDEX = 4;
	public static final int FLIGHT_NUM_INDEX = 5;
	public static final int ORIGIN_INDEX = 6;
	public static final int DEST_INDEX = 7;
	public static final int CRC_DEPART_TIME_INDEX = 8;
	public static final int DEPART_TIME_INDEX = 9;
	public static final int DEP_DELAY_15_INDEX = 10;
	public static final int CRC_ARRIVE_TIME_INDEX = 11;
	public static final int ARRIVE_TIME_INDEX = 12;
	public static final int ARR_DELAY_INDEX = 13;
	public static final int ARR_DELAY_15_INDEX = 14;
	public static final int CANCELLED_INDEX = 15;

	public static boolean isValidData(String[] data) {
		return data != null && data.length > CANCELLED_INDEX;
	}
	
	public static void readInputFiles(Job job, String path, int startYear, int endYear) throws IllegalArgumentException, IOException {
		for (int year = startYear; year <= endYear; year++) {
			StringBuilder builder = new StringBuilder();
			builder.append(path);
			builder.append(year);
			builder.append(".csv");
	        FileInputFormat.addInputPath(job, new Path(builder.toString()));			
		}
	}
	
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
