package mp2.capstone;

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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class CassieAirportCarriers extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(CassieAirportCarriers.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CassieAirportCarriers(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/transp-data/aviation/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Cassie Airport Carriers");
        jobA.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);
        jobA.setMapOutputKeyClass(Text.class);
        jobA.setMapOutputValueClass(IntWritable.class);

        jobA.setMapperClass(AirportCountMap.class);
        jobA.setReducerClass(AirportCountReduce.class);
        jobA.setCombinerClass(AirportCountCombiner.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        
        String query = "UPDATE mp2.airport_carrier_ontime_departs SET "
        		+ "ontime_departs = ?, origin_name = ?, airline_name = ?";
        CqlConfigHelper.setOutputCql(jobA.getConfiguration(), query);
        ConfigHelper.setOutputColumnFamily(jobA.getConfiguration(), "mp2", "airport_carrier_ontime_departs");
        ConfigHelper.setOutputInitialAddress(jobA.getConfiguration(), args[1]);
        ConfigHelper.setOutputPartitioner(jobA.getConfiguration(), "Murmur3Partitioner");
        jobA.setOutputFormatClass(CqlOutputFormat.class);

        jobA.setJarByClass(CassieAirportCarriers.class);
        return jobA.waitForCompletion(true) ? 0 : 1;
    }

    public static class AirportCountMap extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String[] values = value.toString().split(",");
        	if (values != null) {
        		String airportAirlineKey = values[Util.ORIGIN_INDEX] + ' ' + values[Util.AIRLINE_ID_INDEX];
        		try {
        			Double delayMinutes = Double.parseDouble(values[Util.DEP_DELAY_15_INDEX]);
        			if (delayMinutes <= 0) {
        				context.write(new Text(airportAirlineKey), new IntWritable(1));
        			}
        		} catch (NumberFormatException nfe) {
        			// just ignore
        		}
        	}
        }
    }

    public static class AirportCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int count = 0;
        	
        	for (IntWritable value : values) {
       			count += value.get();
        	}
       		context.write(key, new IntWritable(count));
        }
    }

    public static class AirportCountReduce extends Reducer<Text, IntWritable, Map<String, ByteBuffer>, List<ByteBuffer>> {
        Map<String, String> airlineIdToNameMap = new HashMap<>();
        Map<String, String> airportIdToNameMap = new HashMap<>();

    	@Override
    	public void setup(Context context) throws IOException,InterruptedException  {
            Configuration conf = context.getConfiguration();
            airportIdToNameMap = Util.loadAirportNames(conf);
            airlineIdToNameMap = Util.loadAirlineNames(conf);
    	}
    	
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int count = 0;
        	
        	for (IntWritable value : values) {
       			count += value.get();
        	}
        	
        	Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
        	String[] keyValues = key.toString().split(" ");
        	keys.put("origin", ByteBufferUtil.bytes(keyValues[0]));
        	keys.put("airline_id", ByteBufferUtil.bytes(keyValues[1]));
        	
        	List<ByteBuffer> variableValues = new ArrayList<>();
        	variableValues.add(ByteBufferUtil.bytes(count));
        	String airportName = airportIdToNameMap.get(keyValues[0]);
        	String airlineName = airlineIdToNameMap.get(keyValues[1]);
        	variableValues.add(airportName != null ? ByteBufferUtil.bytes(airportName) : ByteBufferUtil.bytes(""));
        	variableValues.add(airlineName != null ? ByteBufferUtil.bytes(airlineName) : ByteBufferUtil.bytes(""));
        	
        	context.write(keys, variableValues);
        }
    }

}

