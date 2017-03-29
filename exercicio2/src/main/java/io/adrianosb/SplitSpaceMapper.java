package io.adrianosb;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.join.TupleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author adriano
 */
public class SplitSpaceMapper extends Mapper<Object, Text, Text, TupleWritable> {

    private final static IntWritable ONE = new IntWritable(1);

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String[] arrValue = StringUtils.split(value.toString());
        for (int i = 0; i < arrValue.length; i++) {
            Text word = new Text(arrValue[i]);
            Text path = new Text(((FileSplit) context.getInputSplit()).getPath().toString());

            //Tem valor numerico no proximo valor?
            if ((i + 1) < arrValue.length && StringUtils.isNumeric(arrValue[i + 1])) {
                Writable[] array = getArray(path, arrValue[i + 1]);
                context.write(word, new TupleWritable(array));
                i++;
            } else {
                Writable[] array = {path, ONE};
                context.write(word, new TupleWritable(array));
            }
        }
    }

    private Writable[] getArray(Text path, String value) throws NumberFormatException {
        IntWritable sum = new IntWritable(Integer.parseInt(value));
        Writable[] array = {path, sum};
        return array;
    }

}
