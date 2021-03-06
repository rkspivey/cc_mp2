package mp2.capstone;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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

import java.io.IOException;
import java.lang.Integer;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

public class TopOnTimeAirlines extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(TopOnTimeAirlines.class);

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopOnTimeAirlines(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/transp-data/aviation/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Airline On-time Arrival Count");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);
        jobA.setMapOutputKeyClass(Text.class);
        jobA.setMapOutputValueClass(IntWritable.class);

        jobA.setMapperClass(OnTimeArrivalCountMap.class);
        jobA.setReducerClass(OnTimeArrivalCountReduce.class);
        jobA.setCombinerClass(OnTimeArrivalCountCombiner.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopOnTimeAirlines.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top On-time Arrival Airlines");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(TopOnTimeArrivalAirlinesMap.class);
        jobB.setReducerClass(TopOnTimeArrivalAirlinesReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopOnTimeAirlines.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class OnTimeArrivalCountMap extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String[] values = value.toString().split(",");
        	if (values != null) {
        		String airlineId = values[Util.AIRLINE_ID_INDEX];
        		try {
        			Double delayMinutes = Double.parseDouble(values[Util.ARR_DELAY_15_INDEX]);
            		if (delayMinutes <= 0) {
                		context.write(new Text(airlineId), new IntWritable(1));
            		}
        		} catch (NumberFormatException nfe) {
        			// just ignore
        		}
        	}
        }
    }

    public static class OnTimeArrivalCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int count = 0;
        	
        	for (IntWritable value : values) {
       			count += value.get();
        	}
       		context.write(key, new IntWritable(count));
        }
    }

    public static class OnTimeArrivalCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int count = 0;
        	
        	for (IntWritable value : values) {
       			count += value.get();
        	}
       		context.write(key, new IntWritable(count));
        }
    }

    public static class TopOnTimeArrivalAirlinesMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        Integer N;
        TreeSet<Pair<Integer, String>> countToAirlineMap = new TreeSet<Pair<Integer, String>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	Integer count = Integer.parseInt(value.toString());
        	String airlineId = key.toString();

        	countToAirlineMap.add(
        		new	Pair<Integer, String>(count, airlineId));
        	if (countToAirlineMap.size() > N) {
        		countToAirlineMap.remove(countToAirlineMap.first());
        	}
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	for (Pair<Integer, String> item : countToAirlineMap) {
        		String[] strings = {item.second, item.first.toString()};
        		TextArrayWritable val = new TextArrayWritable(strings);
        		context.write(NullWritable.get(), val);
        	} 
        }
    }

    public static class TopOnTimeArrivalAirlinesReduce extends Reducer<NullWritable, TextArrayWritable, Text, IntWritable> {
        Integer N;
        TreeSet<Pair<Integer, String>> countToAirlineMap = new TreeSet<Pair<Integer, String>>();
        Map<String, String> airlineIdToNameMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
            airlineIdToNameMap = Util.loadAirlineNames(conf);
        }

        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        	for (TextArrayWritable val: values) {
        		Text[] pair= (Text[]) val.toArray();
        		String airlineId = pair[0].toString();
            	String airlineName = airlineIdToNameMap.get(airlineId);
            	if (airlineName == null) {
            		airlineName = airlineId;
            	}
        		Integer count = Integer.parseInt(pair[1].toString());
        		countToAirlineMap.add(new Pair<Integer, String>(count, airlineName));
        		if (countToAirlineMap.size() > N) {
        			countToAirlineMap.remove(countToAirlineMap.first());
        		}
        	}
        	for (Pair<Integer, String> item: countToAirlineMap) {
        		String airlineName = item.second;
        		IntWritable value = new IntWritable(item.first);
        		context.write(new Text(airlineName), value);
        	}
        }
    }
}

