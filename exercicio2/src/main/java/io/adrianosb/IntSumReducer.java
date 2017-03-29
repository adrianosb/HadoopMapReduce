package io.adrianosb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author adriano
 */
public class IntSumReducer extends Reducer<Text, TupleWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<TupleWritable> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> map = new HashMap<>();
        for (TupleWritable val : values) {
            addMap(map, val);
        }
        StringBuilder result = new StringBuilder();

        map.entrySet().stream().forEach((m) -> {
            result.append(m.getKey()).append(": ").append(m.getValue()).append(";");
        });
        context.write(key, new Text());
    }

    private void addMap(Map<String, Integer> map, TupleWritable val) {
        //TODO
    }

}
