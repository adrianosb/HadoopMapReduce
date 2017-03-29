package io.adrianosb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.joda.time.DateTime;

/**
 *
 * @author adriano
 */
public class WordCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordCount(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        // Create job
        Job job = Job.getInstance(getConf(), "WordCount");
        job.setJarByClass(WordCount.class);
        
        // Setup MapReduce job
        // Do not specify the number of Reducer
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        
        // Specify key / value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        // Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // Output
        FileOutputFormat.setOutputPath(job, new Path(args[1], DateTime.now().toString("yyyy_MM_dd_HH_mm_ss")));
        
        // Execute job and return status
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
