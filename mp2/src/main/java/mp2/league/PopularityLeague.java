package mp2.league;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.*;

public class PopularityLeague extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new PopularityLeague(), args);
        System.exit(res);
    }

    public static class IntArrayWritable extends ArrayWritable {
        public IntArrayWritable() {
            super(IntWritable.class);
        }

        public IntArrayWritable(Integer[] numbers) {
            super(IntWritable.class);
            IntWritable[] ints = new IntWritable[numbers.length];
            for (int i = 0; i < numbers.length; i++) {
                ints[i] = new IntWritable(numbers[i]);
            }
            set(ints);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path tmpPath = new Path("/mp2/tmp");
        fs.delete(tmpPath, true);

        Job jobA = Job.getInstance(conf, "Link Count");
        jobA.setOutputKeyClass(IntWritable.class);
        jobA.setOutputValueClass(IntWritable.class);
        jobA.setMapOutputKeyClass(IntWritable.class);
        jobA.setMapOutputValueClass(IntWritable.class);

        jobA.setMapperClass(LinkCountMap.class);
        jobA.setReducerClass(LinkCountReduce.class);

        FileInputFormat.setInputPaths(jobA, new Path(args[0]));
        FileOutputFormat.setOutputPath(jobA, tmpPath);

        jobA.setJarByClass(PopularityLeague.class);
        jobA.waitForCompletion(true);

        Job jobB = Job.getInstance(conf, "League");
        jobB.setOutputKeyClass(IntWritable.class);
        jobB.setOutputValueClass(IntWritable.class);

        jobB.setMapOutputKeyClass(NullWritable.class);
        jobB.setMapOutputValueClass(IntArrayWritable.class);

        jobB.setMapperClass(LeagueRankMap.class);
        jobB.setReducerClass(LeagueRankReduce.class);
        jobB.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(jobB, tmpPath);
        FileOutputFormat.setOutputPath(jobB, new Path(args[1]));

        jobB.setInputFormatClass(KeyValueTextInputFormat.class);
        jobB.setOutputFormatClass(TextOutputFormat.class);

        jobB.setJarByClass(PopularityLeague.class);
        return jobB.waitForCompletion(true) ? 0 : 1;
    }

    public static class LinkCountMap extends Mapper<Object, Text, IntWritable, IntWritable> {
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	StringTokenizer tokenizer = new StringTokenizer(value.toString(), ":\r\n\t ");
        	String pageIdStr = tokenizer.hasMoreTokens() ? tokenizer.nextToken() : null;
        	Integer pageId = pageIdStr != null ? Integer.parseInt(pageIdStr) : null;
        	context.write(new IntWritable(pageId), new IntWritable(pageId));
        	while (tokenizer.hasMoreTokens()) {
        		String token = tokenizer.nextToken();
            	Integer linkId = token != null ? Integer.parseInt(token) : null;
        		context.write(new IntWritable(linkId), new IntWritable(pageId));
        	}
        }
    }

    public static class LinkCountReduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	int count = 0;
        	
        	for (IntWritable value : values) {
        		if (value.get() != key.get()) {
        			count++;
        		}
        	}
       		context.write(key, new IntWritable(count));
        }
    }

    public static class LeagueRankMap extends Mapper<Text, Text, NullWritable, IntArrayWritable> {
    	static Log log = LogFactory.getLog(LeagueRankMap.class);
        Set<Integer> leagueIdSet = new HashSet<>();
        TreeSet<Pair<Integer, Integer>> countToPageMap = new TreeSet<Pair<Integer, Integer>>();

        @Override
        protected void setup(Context context) throws IOException,InterruptedException {
            Configuration conf = context.getConfiguration();
            Path path = new Path(conf.get("league"));
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream inputStream = fs.open(path);
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
            String line = br.readLine();
            while (line != null) {
            	line = line.trim();
            	leagueIdSet.add(Integer.parseInt(line));
            	line = br.readLine();
            }
            br.close();
            
            for (Integer leagueId : leagueIdSet) {
            	log.info("member of league: " + leagueId);
            }
        }

        @Override
        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        	Integer count = Integer.parseInt(value.toString());
        	Integer pageId = Integer.parseInt(key.toString());
        	if (leagueIdSet.contains(pageId)) {
            	countToPageMap.add(
                		new	Pair<Integer, Integer>(count, pageId));        		
        	}
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
        	for (Pair<Integer, Integer> item : countToPageMap) {
        		Integer[] ints = {item.second, item.first};
        		IntArrayWritable val = new IntArrayWritable(ints);
        		context.write(NullWritable.get(), val);
        	} 
        }
    }

    public static class LeagueRankReduce extends Reducer<NullWritable, IntArrayWritable, IntWritable, IntWritable> {
        TreeSet<Pair<Integer, Integer>> countToPageMap = new TreeSet<Pair<Integer, Integer>>();

        @Override
        public void reduce(NullWritable key, Iterable<IntArrayWritable> values, Context context) throws IOException, InterruptedException {
        	List<Pair<Integer, Integer>> valuesList = new ArrayList<>();
        	for (IntArrayWritable val : values) {
        		IntWritable[] pair = (IntWritable[]) val.toArray();
        		Integer pageId = pair[0].get();
        		Integer pageCount = pair[1].get();
        		valuesList.add(new Pair<Integer, Integer>(pageId, pageCount));
        	}
        	
        	for (Pair<Integer, Integer> pair: valuesList) {
        		int rankCount = 0;
        		for (Pair<Integer, Integer> pair2 : valuesList) {
            		if (pair2.second < pair.second) {
            			rankCount++;
            		}
        		}
        		context.write(new IntWritable(pair.first), new IntWritable(rankCount));
        	}
        }
    }
}

// >>> Don't Change
class Pair<A extends Comparable<? super A>,
        B extends Comparable<? super B>>
        implements Comparable<Pair<A, B>> {

    public final A first;
    public final B second;

    public Pair(A first, B second) {
        this.first = first;
        this.second = second;
    }

    public static <A extends Comparable<? super A>,
            B extends Comparable<? super B>>
    Pair<A, B> of(A first, B second) {
        return new Pair<A, B>(first, second);
    }

    @Override
    public int compareTo(Pair<A, B> o) {
        int cmp = o == null ? 1 : (this.first).compareTo(o.first);
        return cmp == 0 ? (this.second).compareTo(o.second) : cmp;
    }

    @Override
    public int hashCode() {
        return 31 * hashcode(first) + hashcode(second);
    }

    private static int hashcode(Object o) {
        return o == null ? 0 : o.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Pair))
            return false;
        if (this == obj)
            return true;
        return equal(first, ((Pair<?, ?>) obj).first)
                && equal(second, ((Pair<?, ?>) obj).second);
    }

    private boolean equal(Object o1, Object o2) {
        return o1 == o2 || (o1 != null && o1.equals(o2));
    }

    @Override
    public String toString() {
        return "(" + first + ", " + second + ')';
    }
}
// <<< Don't Change    
    
