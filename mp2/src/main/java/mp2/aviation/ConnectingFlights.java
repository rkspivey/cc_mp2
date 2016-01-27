package mp2.aviation;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import au.com.bytecode.opencsv.CSVReader;

import java.io.IOException;
import java.io.StringReader;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class ConnectingFlights extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(ConnectingFlights.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ConnectingFlights(), args);
        System.exit(res);
    }
    
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/transp-data/aviation/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Connecting Flights");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(Text.class);
        jobA.setMapOutputKeyClass(Text.class);
        jobA.setMapOutputValueClass(Text.class);
        jobA.setMapperClass(ConnectingFlightMap.class);
        jobA.setReducerClass(ConnectingFlightReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(ConnectingFlights.class);
        jobA.waitForCompletion(true);;

        Job jobB = Job.getInstance(conf, "Optimize Arrival Time");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(Text.class);
        jobB.setMapOutputKeyClass(Text.class);
        jobB.setMapOutputValueClass(Text.class);
        jobB.setMapperClass(OptimizeArrivalDelayMap.class);
        jobB.setReducerClass(OptimizeArrivalDelayReduce.class);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(ConnectingFlights.class);
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
	   					valueBuilder.append(' ');
	   					valueBuilder.append(destFlight.getDate());
	   					valueBuilder.append(' ');
       	   	   			valueBuilder.append(originFlight.getFlightNumber());
       					valueBuilder.append(' ');
       	   	   			valueBuilder.append(destFlight.getFlightNumber());
       					valueBuilder.append(' ');
       	   	   			valueBuilder.append(originFlight.getArrivalTime());
       					valueBuilder.append(' ');
       	   	   			valueBuilder.append(destFlight.getArrivalTime());
       					valueBuilder.append(' ');
       	   	   			valueBuilder.append(originFlight.getArrivalDelay());
       					valueBuilder.append(' ');
       	   	   			valueBuilder.append(destFlight.getArrivalDelay());

	   					StringBuilder keyBuilder = new StringBuilder();
       					keyBuilder.append(originFlight.getOrigin());
       					keyBuilder.append(' ');
       					keyBuilder.append(originFlight.getDest());
       					keyBuilder.append(' ');
       					keyBuilder.append(destFlight.getDest());

       					context.write(new Text(keyBuilder.toString()), new Text(valueBuilder.toString())); 
        			}
        		}
        	}
        }
    }

    public static class OptimizeArrivalDelayMap extends Mapper<Text, Text, Text, Text> {
    	Map<Text, Text> flights = new HashMap<>();
    	Map<Text, Integer> arrivalDelays = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
    		String[] stringValues = value.toString().split(" ");
    		int leg1ArrivalDelay = Integer.parseInt(stringValues[stringValues.length - 2]);
    		int leg2ArrivalDelay = Integer.parseInt(stringValues[stringValues.length - 1]);
    		Integer lowestDelay = arrivalDelays.get(key);
    		if (lowestDelay == null || lowestDelay > leg1ArrivalDelay + leg2ArrivalDelay) {
    			flights.put(key, value);
    			arrivalDelays.put(key, leg1ArrivalDelay + leg2ArrivalDelay);
    		}
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	for (Map.Entry<Text, Text> entry : flights.entrySet()) {
        		context.write(entry.getKey(), entry.getValue());
        	} 
        }
    }

    public static class OptimizeArrivalDelayReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	int totalDelay = 0;
        	Text optimalFlights = null;
        	
        	for (Text value : values) {
        		String[] stringValues = value.toString().split(" ");
        		int leg1ArrivalDelay = Integer.parseInt(stringValues[stringValues.length - 2]);
        		int leg2ArrivalDelay = Integer.parseInt(stringValues[stringValues.length - 1]);
        		if (optimalFlights == null || totalDelay > leg1ArrivalDelay + leg2ArrivalDelay) {
        			totalDelay = leg1ArrivalDelay + leg2ArrivalDelay;
        			optimalFlights = value;
        		}
        	}
        	
        	if (optimalFlights != null) {
        		context.write(key, optimalFlights);
        	}
        }
    }

}

