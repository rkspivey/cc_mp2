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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CassieAirportDestinations extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(CassieAirportDestinations.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CassieAirportDestinations(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/transp-data/aviation/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Cassie Airport Destinations");
        jobA.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(Text.class);
        jobA.setMapOutputKeyClass(Text.class);
        jobA.setMapOutputValueClass(IntArrayWritable.class);
        jobA.setMapperClass(AirportCountMap.class);
        jobA.setReducerClass(AirportCountReduce.class);
        jobA.setCombinerClass(AirportCountCombiner.class);

        Util.readInputFiles(jobA, args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        
        String query = "UPDATE " + args[4] + ".airport_dest_ontime SET "
        		+ "origin_name = ?, destination_name = ?, ontime_count = ?, total_count = ?";
        CqlConfigHelper.setOutputCql(jobA.getConfiguration(), query);
        ConfigHelper.setOutputColumnFamily(jobA.getConfiguration(), args[4], "airport_dest_ontime");
        ConfigHelper.setOutputInitialAddress(jobA.getConfiguration(), args[3]);
        ConfigHelper.setOutputPartitioner(jobA.getConfiguration(), "Murmur3Partitioner");
        jobA.setOutputFormatClass(CqlOutputFormat.class);

        jobA.setJarByClass(CassieAirportDestinations.class);
        return jobA.waitForCompletion(true) ? 0 : 1;
    }

    public static class AirportCountMap extends Mapper<Object, Text, Text, IntArrayWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String[] values = value.toString().split(",", -1);
        	if (Util.isValidData(values)) {
        		String airportDestKey = values[Util.ORIGIN_INDEX] + ' ' + values[Util.DEST_INDEX];
    			Integer[] outputValues = new Integer[2];
        		try {
        			Double delayMinutes = Double.parseDouble(values[Util.DEP_DELAY_15_INDEX]);
            		if (delayMinutes <= 0) {
                		outputValues[0] = 1;
            		} else {
            			outputValues[0] = 0;
            		}
        		} catch (NumberFormatException nfe) {
        			// this means the field is not set, so the flight was cancelled
        			outputValues[0] = 0;
        		}
        		outputValues[1] = 1;
    			context.write(new Text(airportDestKey), new IntArrayWritable(outputValues));
        	}
        }
    }

    public static class AirportCountCombiner extends Reducer<Text, IntArrayWritable, Text, IntArrayWritable> {
        @Override
        public void reduce(Text key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
        	Integer[] counts = new Integer[2];
        	counts[0] = 0;
        	counts[1] = 0;
        	
        	for (IntArrayWritable value : values) {
        		Writable[] wvalues = value.get();
        		if (wvalues != null && wvalues.length == 2) {
        			counts[0] += ((IntWritable) wvalues[0]).get();
        			counts[1] += ((IntWritable) wvalues[1]).get();
        		}
        	}
       		context.write(key, new IntArrayWritable(counts));
        }
    }

    public static class AirportCountReduce extends Reducer<Text, IntArrayWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {
        Map<String, String> airportIdToNameMap = new HashMap<>();

    	@Override
    	public void setup(Context context) throws IOException,InterruptedException  {
            Configuration conf = context.getConfiguration();
            airportIdToNameMap = Util.loadAirportNames(conf);
    	}
    	
        @Override
        public void reduce(Text key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
        	int onTimeCount = 0;
        	int totalCount = 0;
        	
        	for (IntArrayWritable value : values) {
        		Writable[] wvalues = value.get();
        		if (wvalues != null && wvalues.length == 2) {
        			onTimeCount += ((IntWritable) wvalues[0]).get();
        			totalCount += ((IntWritable) wvalues[1]).get();
        		}
        	}
        	
        	Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
        	String[] keyValues = key.toString().split(" ");
        	keys.put("origin", ByteBufferUtil.bytes(keyValues[0]));
        	keys.put("destination", ByteBufferUtil.bytes(keyValues[1]));
        	keys.put("ontime_percentage", ByteBufferUtil.bytes((double) onTimeCount / (double) totalCount));
        	
        	List<ByteBuffer> variableValues = new ArrayList<>();
        	String originAirportName = airportIdToNameMap.get(keyValues[0]);
        	String destAirportName = airportIdToNameMap.get(keyValues[1]);
        	variableValues.add(originAirportName != null ? ByteBufferUtil.bytes(originAirportName) : ByteBufferUtil.bytes(""));
        	variableValues.add(destAirportName != null ? ByteBufferUtil.bytes(destAirportName) : ByteBufferUtil.bytes(""));
        	variableValues.add(ByteBufferUtil.bytes(onTimeCount));
        	variableValues.add(ByteBufferUtil.bytes(totalCount));
        	
        	context.write(keys, variableValues);
        }
    }

}

