package mp2.aviation;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
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
import java.util.Comparator;
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
        jobA.setOutputValueClass(Text.class);
        jobA.setMapOutputKeyClass(Text.class);
        jobA.setMapOutputValueClass(IntArrayWritable.class);
        jobA.setMapperClass(OnTimeArrivalCountMap.class);
        jobA.setReducerClass(OnTimeArrivalCountReduce.class);
        jobA.setCombinerClass(OnTimeArrivalCountCombiner.class);

        Util.readInputFiles(jobA, args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
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
        FileOutputFormat.setOutputPath(jobB, new Path(args[3]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopOnTimeAirlines.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class OnTimeArrivalCountMap extends Mapper<Object, Text, Text, IntArrayWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String[] values = value.toString().split(",", -1);
        	if (Util.isValidData(values)) {
        		String airlineId = values[Util.AIRLINE_ID_INDEX];
    			Integer[] outputValues = new Integer[2];
        		try {
        			Double delayMinutes = Double.parseDouble(values[Util.ARR_DELAY_15_INDEX]);
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
    			context.write(new Text(airlineId), new IntArrayWritable(outputValues));
        	}
        }
    }

    public static class OnTimeArrivalCountCombiner extends Reducer<Text, IntArrayWritable, Text, IntArrayWritable> {
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

    public static class OnTimeArrivalCountReduce extends Reducer<Text, IntArrayWritable, Text, Text> {
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
        	
        	StringBuilder builder = new StringBuilder();
        	builder.append(counts[0]);
        	builder.append(' ');
        	builder.append(counts[1]);
       		context.write(key, new Text(builder.toString()));
        }
    }

    public static class PairComparator implements Comparator<Pair<OnTimeStats, String>> {

		@Override
		public int compare(Pair<OnTimeStats, String> o1, Pair<OnTimeStats, String> o2) {
			return o2.first.compareTo(o1.first);
		}
    	
    }

    public static class TopOnTimeArrivalAirlinesMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        Integer N;
        TreeSet<Pair<OnTimeStats, String>> countToAirlineMap = new TreeSet<Pair<OnTimeStats, String>>(new PairComparator());

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	String[] valueStrings = value.toString().split(" ");
        	Integer onTimeCount = Integer.parseInt(valueStrings[0]);
        	Integer totalCount = Integer.parseInt(valueStrings[1]);
        	String airlineId = key.toString();

        	countToAirlineMap.add(
        		new	Pair<OnTimeStats, String>(new OnTimeStats(airlineId, onTimeCount, totalCount), airlineId));
        	if (countToAirlineMap.size() > N) {
        		countToAirlineMap.remove(countToAirlineMap.last());
        	}
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	for (Pair<OnTimeStats, String> item : countToAirlineMap) {
        		String[] strings = {item.second, item.first.toString()};
        		TextArrayWritable val = new TextArrayWritable(strings);
        		context.write(NullWritable.get(), val);
        	} 
        }
    }

    public static class TopOnTimeArrivalAirlinesReduce extends Reducer<NullWritable, TextArrayWritable, Text, Text> {
        Integer N;
        TreeSet<Pair<OnTimeStats, String>> countToAirlineMap = new TreeSet<Pair<OnTimeStats, String>>(new PairComparator());
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
        		OnTimeStats stats = new OnTimeStats(airlineId, pair[1].toString());
        		countToAirlineMap.add(new Pair<OnTimeStats, String>(stats, airlineName));
        		if (countToAirlineMap.size() > N) {
        			countToAirlineMap.remove(countToAirlineMap.last());
        		}
        	}
        	for (Pair<OnTimeStats, String> item: countToAirlineMap) {
        		String airlineName = item.second;
        		context.write(new Text(airlineName), new Text(item.first.toString()));
        	}
        }
    }
}

