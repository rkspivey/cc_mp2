package mp2.aviation;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

public class IntArrayWritable extends ArrayWritable {
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

