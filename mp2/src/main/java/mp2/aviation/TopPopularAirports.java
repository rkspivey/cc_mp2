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

public class TopPopularAirports extends Configured implements Tool {
    public static final Log LOG = LogFactory.getLog(TopPopularAirports.class);
    
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TopPopularAirports(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/transp-data/aviation/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Airport Count");
        jobA.setOutputKeyClass(Text.class);
        jobA.setOutputValueClass(IntWritable.class);
        jobA.setMapOutputKeyClass(Text.class);
        jobA.setMapOutputValueClass(IntWritable.class);

        jobA.setMapperClass(AirportCountMap.class);
        jobA.setReducerClass(AirportCountReduce.class);
        jobA.setCombinerClass(AirportCountCombiner.class);

        Util.readInputFiles(jobA, args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopPopularAirports.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Airports");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(Text.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(TopAirportsMap.class);
        jobB.setReducerClass(TopAirportsReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[3]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopPopularAirports.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class AirportCountMap extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	String[] values = value.toString().split(",", -1);
        	if (Util.isValidData(values)) {
        		String origin = values[Util.ORIGIN_INDEX];
        		if (origin != null && !origin.isEmpty()) {
            		context.write(new Text(origin), new IntWritable(1));
        		}
        		
        		String dest = values[Util.DEST_INDEX];
        		if (dest != null && !dest.isEmpty()) {
            		context.write(new Text(dest), new IntWritable(1));        			
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

    public static class TopAirportsMap extends Mapper<Text, Text, NullWritable, TextArrayWritable> {
        Integer N;
        TreeSet<Pair<CountStat, String>> countToAirportMap = new TreeSet<Pair<CountStat, String>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	Integer count = Integer.parseInt(value.toString());
        	String airportId = key.toString();
        	countToAirportMap.add(
        		new	Pair<CountStat, String>(new CountStat(airportId, count), airportId));
        	if (countToAirportMap.size() > N) {
        		countToAirportMap.remove(countToAirportMap.last());
        	}
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	for (Pair<CountStat, String> item : countToAirportMap) {
        		String[] strings = {item.second, item.first.toString()};
        		TextArrayWritable val = new TextArrayWritable(strings);
        		context.write(NullWritable.get(), val);
        	} 
        }
    }

    public static class TopAirportsReduce extends Reducer<NullWritable, TextArrayWritable, Text, Text> {
        Integer N;
        TreeSet<Pair<CountStat, String>> countToAirportMap = new TreeSet<Pair<CountStat, String>>();
        Map<String, String> airportIdToNameMap = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            this.N = conf.getInt("N", 10);
            airportIdToNameMap = Util.loadAirportNames(conf);
        }

        @Override
        public void reduce(NullWritable key, Iterable<TextArrayWritable> values, Context context) throws IOException, InterruptedException {
        	for (TextArrayWritable val: values) {
        		Text[] pair= (Text[]) val.toArray();
        		String airportId = pair[0].toString();
        		String airportName = airportIdToNameMap.get(airportId);
        		if (airportName == null) {
        			airportName = airportId;
        		}
        		Integer count = Integer.parseInt(pair[1].toString());
        		countToAirportMap.add(new Pair<CountStat, String>(new CountStat(airportName, count), airportId));
        		if (countToAirportMap.size() > N) {
        			countToAirportMap.remove(countToAirportMap.first());
        		}
        	}
        	for (Pair<CountStat, String> item: countToAirportMap) {
        		String airportId = item.second;
        		String airportName = airportIdToNameMap.get(airportId);
        		context.write(new Text(airportId + " " + airportName), new Text(item.first.toString()));
        	}
        }
    }
}

