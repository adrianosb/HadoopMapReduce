package io.adrianosb;

import java.io.IOException;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 *
 * @author adriano
 */
public class MyMapper extends Mapper<Object, Text, Text, TuplaWritable> {

    @Override
    public void map(Object key, Text value, Mapper.Context context) throws IOException, InterruptedException {

        String[] arrValue = StringUtils.split(value.toString());
        for (int i = 0; i < arrValue.length; i++) {
            Text word = new Text(arrValue[i]);
            String path = ((FileSplit) context.getInputSplit()).getPath().toString();

            //Tem valor numerico no proximo valor?
            if ((i + 1) < arrValue.length && StringUtils.isNumeric(arrValue[i + 1])) {
                context.write(word, new TuplaWritable(path, Integer.valueOf(arrValue[i + 1])));
                i++;
            } else {
                context.write(word, new TuplaWritable(path, 1));
            }
        }
    }

}
