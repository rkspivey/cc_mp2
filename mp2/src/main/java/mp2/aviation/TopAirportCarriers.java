package mp2.aviation;

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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.lang.Integer;
import java.util.HashMap;
import java.util.Map;

public class TopAirportCarriers extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(TopAirportCarriers.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopAirportCarriers(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/transp-data/aviation/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Airport Carriers");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);
        jobA.setMapOutputKeyClass(Text.class);
        jobA.setMapOutputValueClass(IntWritable.class);

        jobA.setMapperClass(AirportCountMap.class);
        jobA.setReducerClass(AirportCountReduce.class);
        jobA.setCombinerClass(AirportCountCombiner.class);

        Util.readInputFiles(jobA, args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        FileOutputFormat.setOutputPath(jobA, new Path(args[3]));

        jobA.setJarByClass(TopAirportCarriers.class);
        return jobA.waitForCompletion(true) ? 0 : 1;
    }

    public static class AirportCountMap extends Mapper<Object, Text, Text, IntWritable> {
        Integer N;
        Map<String, String> airlineIdToNameMap = new HashMap<>();
        Map<String, String> airportIdToNameMap = new HashMap<>();
    	
    	@Override
    	public void setup(Context context) throws IOException,InterruptedException  {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
            airportIdToNameMap = Util.loadAirportNames(conf);
            airlineIdToNameMap = Util.loadAirlineNames(conf);
    	}
    	
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String[] values = value.toString().split(",", -1);
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

    public static class AirportCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int count = 0;
        	
        	for (IntWritable value : values) {
       			count += value.get();
        	}
       		context.write(key, new IntWritable(count));
        }
    }

}

