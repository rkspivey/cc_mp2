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

import au.com.bytecode.opencsv.CSVReader;

import java.io.IOException;
import java.io.StringReader;
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

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(TopPopularAirports.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "Top Airports");
        jobB.setOutputKeyClass(Text.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(TextArrayWritable.class);

        jobB.setMapperClass(TopAirportsMap.class);
        jobB.setReducerClass(TopAirportsReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(TopPopularAirports.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class AirportCountMap extends Mapper<Object, Text, Text, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	StringReader valueReader = new StringReader(value.toString());
        	CSVReader reader = new CSVReader(valueReader);
        	String[] values = reader.readNext();
        	if (values != null) {
        		String origin = values[Util.ORIGIN_INDEX];
        		if (origin != null && !origin.isEmpty()) {
            		context.write(new Text(origin), new IntWritable(1));
        		}
        		
        		String dest = values[Util.DEST_INDEX];
        		if (dest != null && !dest.isEmpty()) {
            		context.write(new Text(dest), new IntWritable(1));        			
        		}
        	}
        	reader.close();
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
        TreeSet<Pair<Integer, String>> countToAirportMap = new TreeSet<Pair<Integer, String>>();

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
        		new	Pair<Integer, String>(count, airportId));
        	if (countToAirportMap.size() > N) {
        		countToAirportMap.remove(countToAirportMap.first());
        	}
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	for (Pair<Integer, String> item : countToAirportMap) {
        		String[] strings = {item.second, item.first.toString()};
        		TextArrayWritable val = new TextArrayWritable(strings);
        		context.write(NullWritable.get(), val);
        	} 
        }
    }

    public static class TopAirportsReduce extends Reducer<NullWritable, TextArrayWritable, Text, IntWritable> {
        Integer N;
        TreeSet<Pair<Integer, String>> countToAirportMap = new TreeSet<Pair<Integer, String>>();
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
        		countToAirportMap.add(new Pair<Integer, String>(count, airportName));
        		if (countToAirportMap.size() > N) {
        			countToAirportMap.remove(countToAirportMap.first());
        		}
        	}
        	for (Pair<Integer, String> item: countToAirportMap) {
        		String airportId = item.second;
        		IntWritable value = new IntWritable(item.first);
        		context.write(new Text(airportId), value);
        	}
        }
    }
}

