package mp2.aviation;

import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper;
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import au.com.bytecode.opencsv.CSVReader;

import java.io.IOException;
import java.io.StringReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
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
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/transp-data/aviation/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Cassie Connecting Flights");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(Text.class);
        jobA.setMapOutputKeyClass(Text.class);
        jobA.setMapOutputValueClass(Text.class);
        jobA.setMapperClass(ConnectingFlightMap.class);
        jobA.setReducerClass(ConnectingFlightReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(CassieConnectingFlights.class);
        jobA.waitForCompletion(true);;

        Job jobB = Job.getInstance(conf, "Cassie Optimize Arrival Time");
        jobB.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(Text.class);
        jobB.setMapOutputKeyClass(Text.class);
        jobB.setMapOutputValueClass(Text.class);
        jobB.setMapperClass(OptimizeArrivalDelayMap.class);
        jobB.setReducerClass(OptimizeArrivalDelayReduce.class);

        FileInputFormat.setInputPaths(jobB, tmpPath);

        String query = "UPDATE mp2.connecting_flights SET "
        		+ "origin_depart_time = ?, origin_arrival_time = ?, "
        		+ "destination_depart_time = ?, destination_arrival_time = ?, "
        		+ "origin_arrival_delay = ?, destination_arrival_delay = ?";
        CqlConfigHelper.setOutputCql(jobB.getConfiguration(), query);
        ConfigHelper.setOutputColumnFamily(jobB.getConfiguration(), "mp2", "connecting_flights");
        ConfigHelper.setOutputInitialAddress(jobB.getConfiguration(), args[1]);
        ConfigHelper.setOutputPartitioner(jobB.getConfiguration(), "Murmur3Partitioner");
        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(CqlOutputFormat.class);

        jobB.setJarByClass(CassieConnectingFlights.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class ConnectingFlightMap extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	StringReader valueReader = new StringReader(value.toString());
        	CSVReader reader = new CSVReader(valueReader);
        	String[] values = reader.readNext();
        	if (values != null && values.length >= 42) {
            	FlightInfo flight = new FlightInfo(values);
            	double cancelled = values != null && values[Util.CANCELLED_INDEX] != null ? Double.parseDouble(values[Util.CANCELLED_INDEX]) : 1;
            	if (cancelled == 0d) {
            		if (flight.getArrivalTime() > 1200) {
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

    public static class ConnectingFlightReduce extends Reducer<Text, Text, Text, Text> {
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
	   					StringBuilder valueBuilder = new StringBuilder();
	   					valueBuilder.append(originFlight.getDate());
	   					valueBuilder.append(',');
	   					valueBuilder.append(destFlight.getDate());
	   					valueBuilder.append(',');
       	   	   			valueBuilder.append(originFlight.getFlightNumber());
       					valueBuilder.append(',');
       	   	   			valueBuilder.append(destFlight.getFlightNumber());
       					valueBuilder.append(',');
       	   	   			valueBuilder.append(originFlight.getDepartTime());
       					valueBuilder.append(',');
       	   	   			valueBuilder.append(destFlight.getDepartTime());
       					valueBuilder.append(',');
       	   	   			valueBuilder.append(originFlight.getArrivalTime());
       					valueBuilder.append(',');
       	   	   			valueBuilder.append(destFlight.getArrivalTime());
       					valueBuilder.append(',');
       	   	   			valueBuilder.append(originFlight.getArrivalDelay());
       					valueBuilder.append(',');
       	   	   			valueBuilder.append(destFlight.getArrivalDelay());

	   					StringBuilder keyBuilder = new StringBuilder();
       					keyBuilder.append(originFlight.getOrigin());
       					keyBuilder.append(',');
       					keyBuilder.append(originFlight.getDest());
       					keyBuilder.append(',');
       					keyBuilder.append(destFlight.getDest());

       					context.write(new Text(keyBuilder.toString()), new Text(valueBuilder.toString())); 
        			}
        		}
        	}
        }
    }

    public static class OptimizeArrivalDelayMap extends Mapper<Text, Text, Text, Text> {
    	Map<String, String> flights = new HashMap<>();
    	Map<String, Integer> arrivalDelays = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    		String[] stringValues = value.toString().split(",");
    		int leg1ArrivalDelay = Integer.parseInt(stringValues[stringValues.length - 2]);
    		int leg2ArrivalDelay = Integer.parseInt(stringValues[stringValues.length - 1]);
    		Integer lowestDelay = arrivalDelays.get(key);
    		if (lowestDelay == null || lowestDelay > leg1ArrivalDelay + leg2ArrivalDelay) {
    			flights.put(key.toString(), value.toString());
    			arrivalDelays.put(key.toString(), leg1ArrivalDelay + leg2ArrivalDelay);
    		}
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	for (Map.Entry<String, String> entry : flights.entrySet()) {
        		context.write(new Text(entry.getKey()), new Text(entry.getValue()));
        	} 
        }
    }

    public static class OptimizeArrivalDelayReduce extends Reducer<Text, Text, Map<String, ByteBuffer>, List<ByteBuffer>> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	int totalDelay = 0;
        	Text optimalFlights = null;
        	
        	for (Text value : values) {
        		String[] stringValues = value.toString().split(",");
        		int leg1ArrivalDelay = Integer.parseInt(stringValues[stringValues.length - 2]);
        		int leg2ArrivalDelay = Integer.parseInt(stringValues[stringValues.length - 1]);
        		if (optimalFlights == null || totalDelay > leg1ArrivalDelay + leg2ArrivalDelay) {
        			totalDelay = leg1ArrivalDelay + leg2ArrivalDelay;
        			optimalFlights = value;
        		}
        	}
        	
        	if (optimalFlights != null) {
            	Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
            	String[] keyValues = key.toString().split(",");
            	String[] textValues = optimalFlights.toString().split(",");
            	
            	keys.put("origin", ByteBufferUtil.bytes(keyValues[0]));
            	keys.put("layover", ByteBufferUtil.bytes(keyValues[1]));
            	keys.put("destination", ByteBufferUtil.bytes(keyValues[2]));
            	keys.put("origin_flight_date", ByteBufferUtil.bytes(Integer.parseInt(textValues[0])));
            	keys.put("destination_flight_date", ByteBufferUtil.bytes(Integer.parseInt(textValues[1])));
            	keys.put("flight_num1", ByteBufferUtil.bytes(Integer.parseInt(textValues[2])));
            	keys.put("flight_num2", ByteBufferUtil.bytes(Integer.parseInt(textValues[3])));
            	
            	List<ByteBuffer> variableValues = new ArrayList<>();
            	variableValues.add(ByteBufferUtil.bytes(Integer.parseInt(textValues[4]))); // origin_depart_time
            	variableValues.add(ByteBufferUtil.bytes(Integer.parseInt(textValues[5]))); // origin_arrival_time
            	variableValues.add(ByteBufferUtil.bytes(Integer.parseInt(textValues[6]))); // destination_depart_time
            	variableValues.add(ByteBufferUtil.bytes(Integer.parseInt(textValues[7]))); // destination_arrival_time
            	variableValues.add(ByteBufferUtil.bytes(Integer.parseInt(textValues[8]))); // origin_arrival_delay
            	variableValues.add(ByteBufferUtil.bytes(Integer.parseInt(textValues[9]))); // destination_arrival_delay
            	
            	context.write(keys, variableValues);
        	}
        }
    }

}

