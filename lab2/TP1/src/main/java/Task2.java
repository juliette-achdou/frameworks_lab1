/**
* Created by juliette on 11/10/2016.
*/
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;


public class Task2 {
    public static class Mapper2 extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context
        ) throws IOException, InterruptedException {

            String[] result = value.toString().split(";");
            int count = StringUtils.countMatches(result[2], ",") + 1;
            /* the number of origins is the number of "," in this field +1*/
            word.set(String.valueOf(count));
            context.write(word, one);

        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
                        /* we only have to sum the values*/
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "Task2");
        job.setJarByClass(Task1.class);
        job.setMapperClass(Mapper2.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.out.println("Task2");
        System.out.println(args[0]);
        System.out.println(new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}