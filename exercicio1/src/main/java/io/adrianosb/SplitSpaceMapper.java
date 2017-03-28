package io.adrianosb;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *
 * @author adriano
 */
public class SplitSpaceMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable ONE = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String[] arrValue = StringUtils.split(value.toString());
        for (int i = 0; i < arrValue.length; i++) {
            Text word = new Text(arrValue[i]);
            if ((i + 1) < arrValue.length && StringUtils.isNumeric(arrValue[i + 1])) {
                context.write(word, new IntWritable(Integer.parseInt(arrValue[i + 1])));
                i++;
            }
            context.write(word, ONE);
        }
    }

}
