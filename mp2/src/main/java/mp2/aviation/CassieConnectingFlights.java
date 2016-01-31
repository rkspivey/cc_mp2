package mp2.aviation;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import au.com.bytecode.opencsv.CSVReader;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class CassieConnectingFlights extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(CassieConnectingFlights.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CassieConnectingFlights(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();

        Job jobA = Job.getInstance(conf, "Cassie Connecting Flights");
        jobA.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(Text.class);
        jobA.setMapOutputKeyClass(Text.class);
        jobA.setMapOutputValueClass(Text.class);
        jobA.setMapperClass(ConnectingFlightMap.class);
        jobA.setReducerClass(ConnectingFlightReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));

        String query = "UPDATE mp2.connecting_flights SET "
        		+ "origin_depart_time = ?, origin_arrival_time = ?, "
        		+ "destination_depart_time = ?, destination_arrival_time = ?, "
        		+ "origin_arrival_delay = ?, destination_arrival_delay = ?";
        CqlConfigHelper.setOutputCql(jobA.getConfiguration(), query);
        ConfigHelper.setOutputColumnFamily(jobA.getConfiguration(), "mp2", "connecting_flights");
        ConfigHelper.setOutputInitialAddress(jobA.getConfiguration(), args[1]);
        ConfigHelper.setOutputPartitioner(jobA.getConfiguration(), "Murmur3Partitioner");
        jobA.setOutputFormatClass(CqlOutputFormat.class);
        jobA.setJarByClass(CassieConnectingFlights.class);
        return jobA.waitForCompletion(true) ? 0 : 1;
    }

    public static class ConnectingFlightMap extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	StringReader valueReader = new StringReader(value.toString());
        	CSVReader reader = new CSVReader(valueReader);
        	String[] values = reader.readNext();
        	if (Util.isValidData(values)) {
            	FlightInfo flight = new FlightInfo(values);
            	if (!flight.isCancelled()) {
            		if (flight.getDepartTime() > 1200) {
            			context.write(new Text(flight.getOrigin()), new Text("D," + value.toString()));
            		}
            		if (flight.getDepartTime() < 1200) {
            			context.write(new Text(flight.getDest()), new Text("O," + value.toString()));
            		}
            	}        		
        	}
        	reader.close();
        }
    }

    public static class ConnectingFlightReduce extends Reducer<Text, Text, Map<String, ByteBuffer>, List<ByteBuffer>> {
        private int computeDate(int date, int offset) {
        	int year = date / 10000;
        	int month = (date % 10000) / 100;
        	int day = (date % 10000) % 100;

        	int date2 = 0;
        	
        	if (day >= 27) {
        		Calendar c = Calendar.getInstance();
        		c.set(Calendar.YEAR, year);
        		c.set(Calendar.MONTH, month - 1);
        		c.set(Calendar.DAY_OF_MONTH, day);
        		c.add(Calendar.DAY_OF_MONTH, 2);
        		int year2 = c.get(Calendar.YEAR);
        		int month2 = c.get(Calendar.MONTH) + 1;
        		int day2 = c.get(Calendar.DAY_OF_MONTH);
        		date2 = year2 * 10000 + month2 * 100 + day2;
        	} else {
        		date2 = date + 2;
        	}
        	
    		return date2;
    	}
    	
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	Set<FlightInfo> originFlights = new HashSet<>();
        	Set<FlightInfo> destFlights = new HashSet<>();
        	for (Text value : values) {
        		String valueStr = value.toString();
        		Set<FlightInfo> flights = valueStr.charAt(0) == 'D' ? destFlights : originFlights;
            	StringReader valueReader = new StringReader(valueStr.substring(2));
            	CSVReader reader = new CSVReader(valueReader);
            	String[] stringValues = reader.readNext();
        		flights.add(new FlightInfo(stringValues));
            	reader.close();
        	}
        	
        	for (FlightInfo destFlight : destFlights) {
       			for (FlightInfo originFlight : originFlights) {
       				if (computeDate(originFlight.getDate(), 2) == destFlight.getDate() && !originFlight.getOrigin().equals(destFlight.getDest())) {
       	            	Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
       	            	
       	            	keys.put("origin", ByteBufferUtil.bytes(originFlight.getOrigin()));
       	            	keys.put("layover", ByteBufferUtil.bytes(originFlight.getDest()));
       	            	keys.put("destination", ByteBufferUtil.bytes(destFlight.getDest()));
       	            	keys.put("origin_flight_date", ByteBufferUtil.bytes(originFlight.getDate()));
       	            	keys.put("destination_flight_date", ByteBufferUtil.bytes(destFlight.getDate()));
       	            	keys.put("flight_num1", ByteBufferUtil.bytes(originFlight.getFlightNumber()));
       	            	keys.put("flight_num2", ByteBufferUtil.bytes(destFlight.getFlightNumber()));
       	            	keys.put("total_arrival_delay", ByteBufferUtil.bytes(originFlight.getArrivalDelay() + destFlight.getArrivalDelay()));
       	            	
       	            	List<ByteBuffer> variableValues = new ArrayList<>();
       	            	variableValues.add(ByteBufferUtil.bytes(originFlight.getDepartTime())); // origin_depart_time
       	            	variableValues.add(ByteBufferUtil.bytes(originFlight.getArrivalTime())); // origin_arrival_time
       	            	variableValues.add(ByteBufferUtil.bytes(destFlight.getDepartTime())); // destination_depart_time
       	            	variableValues.add(ByteBufferUtil.bytes(destFlight.getArrivalTime())); // destination_arrival_time
       	            	variableValues.add(ByteBufferUtil.bytes(originFlight.getArrivalDelay())); // origin_arrival_delay
       	            	variableValues.add(ByteBufferUtil.bytes(destFlight.getArrivalDelay())); // destination_arrival_delay
       	            	
       	            	context.write(keys, variableValues);
        			}
        		}
        	}
        }
    }

}

