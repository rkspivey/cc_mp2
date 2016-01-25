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

public class CassieSourceDestSchedules extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(CassieSourceDestSchedules.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new CassieSourceDestSchedules(), args);
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
        jobA.setReducerClass(AirportCountReduce.class);
        jobA.setCombinerClass(AirportCountCombiner.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        
        String query = "UPDATE mp2.source_dest_schedules SET "
        		+ "origin = ?, destination = ?, airline_id = ?, flight_date = ?, depart_time = ?, arrival_time = ?, origin_name = ?, destination_name = ?, airline_name = ?";
        CqlConfigHelper.setOutputCql(jobA.getConfiguration(), query);
        ConfigHelper.setOutputColumnFamily(jobA.getConfiguration(), "mp2", "source_dest_schedules");
        ConfigHelper.setOutputInitialAddress(jobA.getConfiguration(), args[1]);
        ConfigHelper.setOutputPartitioner(jobA.getConfiguration(), "Murmur3Partitioner");
        jobA.setOutputFormatClass(CqlOutputFormat.class);

        jobA.setJarByClass(CassieSourceDestSchedules.class);
        return jobA.waitForCompletion(true) ? 0 : 1;
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

    public static class AirportCountReduce extends Reducer<Text, Text, Map<String, ByteBuffer>, List<ByteBuffer>> {
        Map<String, String> airlineIdToNameMap = new HashMap<>();
        Map<String, String> airportIdToNameMap = new HashMap<>();

    	@Override
    	public void setup(Context context) throws IOException,InterruptedException  {
            Configuration conf = context.getConfiguration();
            airportIdToNameMap = Util.loadAirportNames(conf);
            airlineIdToNameMap = Util.loadAirlineNames(conf);
    	}
    	
        @Override
        public void reduce(Text key, Iterable<Text> text, Context context) throws IOException, InterruptedException {
        	String[] values = key.toString().split(",");

        	Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
        	keys.put("flight_num", ByteBufferUtil.bytes(values[Util.FLIGHT_NUM_INDEX]));
        	
        	List<ByteBuffer> variableValues = new ArrayList<>();
        	String originAirportName = airportIdToNameMap.get(values[Util.ORIGIN_INDEX]);
        	String destinationAirportName = airportIdToNameMap.get(values[Util.DEST_INDEX]);
        	String airlineName = airlineIdToNameMap.get(values[Util.AIRLINE_ID_INDEX]);
    		variableValues.add(ByteBufferUtil.bytes(values[Util.ORIGIN_INDEX]));
    		variableValues.add(ByteBufferUtil.bytes(values[Util.DEST_INDEX]));
    		variableValues.add(ByteBufferUtil.bytes(values[Util.AIRLINE_ID_INDEX]));
    		variableValues.add(ByteBufferUtil.bytes(values[Util.DATE_INDEX]));
    		variableValues.add(ByteBufferUtil.bytes(values[Util.CRC_DEPART_TIME_INDEX]));
    		variableValues.add(ByteBufferUtil.bytes(values[Util.CRC_ARRIVE_TIME_INDEX]));
        	variableValues.add(originAirportName != null ? ByteBufferUtil.bytes(originAirportName) : ByteBufferUtil.bytes(""));
        	variableValues.add(destinationAirportName != null ? ByteBufferUtil.bytes(destinationAirportName) : ByteBufferUtil.bytes(""));
        	variableValues.add(airlineName != null ? ByteBufferUtil.bytes(airlineName) : ByteBufferUtil.bytes(""));
        	
        	context.write(keys, variableValues);
        }
    }

}

