package io.adrianosb;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 *
 * @author adriano
 */
public class MyReducer extends Reducer<Text, TuplaWritable, Text, Text> {
    
    private Text _key = new Text();
    private Text _value = new Text();

    @Override
    protected void reduce(Text key, Iterable<TuplaWritable> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> map = new HashMap<>();
        for (TuplaWritable val : values) {
            addMap(map, val);
//            context.write(key, new TuplaWritable(val.getPath(), val.getSum()));
        }
        StringBuilder result = new StringBuilder();
        map.entrySet().stream().forEach((m) -> {
            result.append(m.getKey()).append(": ").append(m.getValue()).append(";");
        });
        _key.set(key.toString());
        _value.set(result.toString());
        context.write(_key, _value);
    }

    private void addMap(Map<String, Integer> map, TuplaWritable val) {
        if (!map.containsKey(val.getPath().toString())) {
            map.put(val.getPath().toString(), Integer.valueOf(val.getSum().toString()));
        } else {
            map.put(val.getPath().toString(), Integer.valueOf(val.getSum().toString()) + map.get(val.getPath().toString()));
        }
    }

}
