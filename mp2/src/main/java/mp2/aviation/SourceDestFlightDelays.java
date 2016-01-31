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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.lang.Integer;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

public class SourceDestFlightDelays extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(SourceDestFlightDelays.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new SourceDestFlightDelays(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/transp-data/aviation/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "SourceDest Flight Delays");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(Text.class);
        jobA.setMapOutputKeyClass(Text.class);
        jobA.setMapOutputValueClass(Text.class);

        jobA.setMapperClass(FlightDelayMap.class);
        jobA.setReducerClass(FlightDelayReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, new Path(args[1]));

        jobA.setJarByClass(SourceDestFlightDelays.class);
        return jobA.waitForCompletion(true) ? 0 : 1;
    }

    public static class FlightDelayMap extends Mapper<Object, Text, Text, Text> {
        Integer N;
        TreeSet<Pair<Integer, String>> countToAirlineMap = new TreeSet<Pair<Integer, String>>();
        Map<String, String> airlineIdToNameMap = new HashMap<>();
        Map<String, String> airportIdToNameMap = new HashMap<>();
    	
    	@Override
    	public void setup(Context context) throws IOException,InterruptedException  {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
            airlineIdToNameMap = Util.loadAirlineNames(conf);
            airportIdToNameMap = Util.loadAirportNames(conf);
    	}
    	
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String[] values = value.toString().split(",", -1);
        	if (values != null) {
        		String airportAirlineKey = values[Util.ORIGIN_INDEX] + ' ' + values[Util.DEST_INDEX] + ' ' + values[Util.DATE_INDEX];
           		context.write(new Text(airportAirlineKey), new Text(values[Util.FLIGHT_NUM_INDEX] + ' ' + values[Util.ARR_DELAY_15_INDEX]));
        	}
        }
    }

    public static class FlightDelayReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        	for (Text value : values) {
           		context.write(key, value);
        	}
        }
    }

}

