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

import java.io.IOException;
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
        jobA.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(Text.class);
        jobA.setMapOutputKeyClass(Text.class);
        jobA.setMapOutputValueClass(Text.class);
        jobA.setMapperClass(ConnectingFlightMap.class);
        jobA.setReducerClass(ConnectingFlightReduce.class);

        Util.readInputFiles(jobA, args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);
        jobA.setJarByClass(CassieConnectingFlights.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Optimize Cassie Connecting Flights");
        jobB.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(Text.class);
        jobB.setMapOutputKeyClass(Text.class);
        jobB.setMapOutputValueClass(Text.class);
        jobB.setMapperClass(OptimizeArrivalDelayMap.class);
        jobB.setReducerClass(OptimizeArrivalDelayReduce.class);
        jobB.setNumReduceTasks(1);
        jobB.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.setInputPaths(jobB, tmpPath);

        String query = "UPDATE " + args[4] + ".connecting_flights SET "
        		+ "flight_num1 = ?, flight_num2= ?, total_arrival_delay = ?";
        CqlConfigHelper.setOutputCql(jobB.getConfiguration(), query);
        ConfigHelper.setOutputColumnFamily(jobB.getConfiguration(), args[4], "connecting_flights");
        ConfigHelper.setOutputInitialAddress(jobB.getConfiguration(), args[3]);
        ConfigHelper.setOutputPartitioner(jobB.getConfiguration(), "Murmur3Partitioner");
        jobB.setOutputFormatClass(CqlOutputFormat.class);
        jobB.setJarByClass(CassieConnectingFlights.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class ConnectingFlightMap extends Mapper<Object, Text, Text, Text> {
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
        		c.add(Calendar.DAY_OF_MONTH, offset);
        		int year2 = c.get(Calendar.YEAR);
        		int month2 = c.get(Calendar.MONTH) + 1;
        		int day2 = c.get(Calendar.DAY_OF_MONTH);
        		date2 = year2 * 10000 + month2 * 100 + day2;
        	} else {
        		date2 = date + offset;
        	}
        	
    		return date2;
    	}
    	

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String[] values = value.toString().split(",", -1);
        	if (Util.isValidData(values)) {
            	FlightInfo flight = new FlightInfo(values);
            	if (!flight.isCancelled()) {
            		if (flight.getDepartTime() > 1200) {
            			context.write(new Text(flight.getOrigin() + computeDate(flight.getDate(), -2)), new Text("D," + value.toString()));
            		}
            		if (flight.getDepartTime() < 1200) {
            			context.write(new Text(flight.getDest() + flight.getDate()), new Text("O," + value.toString()));
            		}
            	}        		
        	}
        }
    }

    public static class ConnectingFlightReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	Set<FlightInfo> originFlights = new HashSet<>();
        	Set<FlightInfo> destFlights = new HashSet<>();
        	for (Text value : values) {
        		String valueStr = value.toString();
        		Set<FlightInfo> flights = valueStr.charAt(0) == 'D' ? destFlights : originFlights;
            	String[] stringValues = valueStr.substring(2).split(",", -1);
        		flights.add(new FlightInfo(stringValues));
        	}
        	
        	Set<Pair<FlightInfo, FlightInfo>> connections = new HashSet<>(); 
        	
        	for (FlightInfo destFlight : destFlights) {
       			for (FlightInfo originFlight : originFlights) {
       				connections.add(new Pair<FlightInfo, FlightInfo>(originFlight, destFlight));
       			}
        	}
        	
        	for (Pair<FlightInfo, FlightInfo> connection : connections) {
        		StringBuilder keyBuilder = new StringBuilder();
        		keyBuilder.append(connection.first.getOrigin());
        		keyBuilder.append(",");
        		keyBuilder.append(connection.first.getDest());
        		keyBuilder.append(",");
        		keyBuilder.append(connection.second.getDest());
        		keyBuilder.append(",");
        		keyBuilder.append(connection.first.getDate());
        		
        		StringBuilder valueBuilder = new StringBuilder();
        		valueBuilder.append(connection.first.getFlightNumber());
        		valueBuilder.append(",");
        		valueBuilder.append(connection.second.getFlightNumber());
        		valueBuilder.append(",");
        		valueBuilder.append(connection.first.getArrivalDelay());
        		valueBuilder.append(",");
        		valueBuilder.append(connection.second.getArrivalDelay());
        		
        		context.write(new Text(keyBuilder.toString()), new Text(valueBuilder.toString()));
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
            	keys.put("origin_flight_date", ByteBufferUtil.bytes(Integer.parseInt(keyValues[3])));
            	
            	List<ByteBuffer> variableValues = new ArrayList<>();
            	variableValues.add(ByteBufferUtil.bytes(Integer.parseInt(textValues[0]))); // flight_num1
            	variableValues.add(ByteBufferUtil.bytes(Integer.parseInt(textValues[1]))); // flight_num2
            	variableValues.add(ByteBufferUtil.bytes(Integer.parseInt(textValues[2]) + Integer.parseInt(textValues[3]))); // total_delay
            	
            	context.write(keys, variableValues);
        	}
        }
    }
}

