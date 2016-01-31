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
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
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

        Util.readInputFiles(jobA, args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        
        String query = "UPDATE " + args[4] + ".source_dest_schedules SET "
        		+ "origin = ?, destination = ?, depart_time = ?, arrival_time = ?";
//        		+ "origin = ?, destination = ?, depart_time = ?, arrival_time = ?, origin_name = ?, destination_name = ?, airline_name = ?";
        CqlConfigHelper.setOutputCql(jobA.getConfiguration(), query);
        ConfigHelper.setOutputColumnFamily(jobA.getConfiguration(), args[4], "source_dest_schedules");
        ConfigHelper.setOutputInitialAddress(jobA.getConfiguration(), args[3]);
        ConfigHelper.setOutputPartitioner(jobA.getConfiguration(), "Murmur3Partitioner");
        jobA.setOutputFormatClass(CqlOutputFormat.class);

        jobA.setJarByClass(CassieSourceDestSchedules.class);
        return jobA.waitForCompletion(true) ? 0 : 1;
    }

    public static class AirportCountReduce extends Reducer<LongWritable, Text, Map<String, ByteBuffer>, List<ByteBuffer>> {
        Map<String, String> airlineIdToNameMap = new HashMap<>();
        Map<String, String> airportIdToNameMap = new HashMap<>();

    	@Override
    	public void setup(Context context) throws IOException,InterruptedException  {
            Configuration conf = context.getConfiguration();
            airportIdToNameMap = Util.loadAirportNames(conf);
            airlineIdToNameMap = Util.loadAirlineNames(conf);
    	}
    	
        @Override
        public void reduce(LongWritable key, Iterable<Text> text, Context context) throws IOException, InterruptedException {
        	Text value = text.iterator().next();
        	String[] values = value.toString().split(",", -1);
        	if (values == null || values[Util.DATE_INDEX] == null || values[Util.DATE_INDEX].trim().length() != 10) {
        		return;
        	}

        	Integer flightDate = 0;
        	Integer flightNum = null;
        	Integer departTime = null;
        	Integer arriveTime = null;
        	
        	try {
        		flightNum = Integer.parseInt(values[Util.FLIGHT_NUM_INDEX]);
        		Integer year = Integer.parseInt(values[Util.YEAR_INDEX]);
        		Integer month = Integer.parseInt(values[Util.MONTH_INDEX]);
        		Integer day = Integer.parseInt(values[Util.DAY_INDEX]);
        		flightDate = year * 10000 + month * 100 + day;
        		departTime = Integer.parseInt(values[Util.CRC_DEPART_TIME_INDEX]);
        		arriveTime = Integer.parseInt(values[Util.CRC_ARRIVE_TIME_INDEX]);
        	} catch (NumberFormatException nfe) {
        		LOG.error("NumberFormatException: " + value.toString() + " " + nfe);
        		return;
        	}
        	

        	Map<String, ByteBuffer> keys = new LinkedHashMap<String, ByteBuffer>();
        	keys.put("airline_id", ByteBufferUtil.bytes(values[Util.AIRLINE_ID_INDEX]));
        	keys.put("flight_date", ByteBufferUtil.bytes(flightDate));
        	keys.put("flight_num", ByteBufferUtil.bytes(flightNum));
        	
        	List<ByteBuffer> variableValues = new ArrayList<>();
        	//String originAirportName = airportIdToNameMap.get(values[Util.ORIGIN_INDEX]);
        	//String destinationAirportName = airportIdToNameMap.get(values[Util.DEST_INDEX]);
        	//String airlineName = airlineIdToNameMap.get(values[Util.AIRLINE_ID_INDEX]);
    		variableValues.add(ByteBufferUtil.bytes(values[Util.ORIGIN_INDEX]));
    		variableValues.add(ByteBufferUtil.bytes(values[Util.DEST_INDEX]));
    		variableValues.add(ByteBufferUtil.bytes(departTime));
    		variableValues.add(ByteBufferUtil.bytes(arriveTime));
        	//variableValues.add(originAirportName != null ? ByteBufferUtil.bytes(originAirportName) : ByteBufferUtil.bytes(""));
        	//variableValues.add(destinationAirportName != null ? ByteBufferUtil.bytes(destinationAirportName) : ByteBufferUtil.bytes(""));
        	//variableValues.add(airlineName != null ? ByteBufferUtil.bytes(airlineName) : ByteBufferUtil.bytes(""));
        	
        	context.write(keys, variableValues);
        }
    }

}

