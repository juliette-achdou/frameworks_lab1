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

import java.io.IOException;


public class Task1 {
    public static class Mapper1 extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context
        ) throws IOException, InterruptedException {

            String[] result = value.toString().split(";");
            String[] origin = result[2].split(",");
            /* for every origin in the field, we set the value to 1*/
            /* there will be empty words, because there are empty origin fields 1*/
            for (String myString : origin) {
                word.set(myString);

                context.write(word, one);
            }


        }
    }

    public static class IntSumReducer
            extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            /* we only have to sum*/
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
        Job job = Job.getInstance(conf, "Task1");
        job.setJarByClass(Task1.class);
        job.setMapperClass(Mapper1.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.out.println("Task1");
        System.out.println(args[0]);
        System.out.println(new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}