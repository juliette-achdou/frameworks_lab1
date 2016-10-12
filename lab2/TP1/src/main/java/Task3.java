


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
import org.apache.hadoop.io.DoubleWritable;

import java.io.IOException;


public class Task3 {
    public static class Mapper3 extends org.apache.hadoop.mapreduce.Mapper<Object, Text, Text, IntWritable> {
        private Text word = new Text();

        public void map(Object key, Text value, org.apache.hadoop.mapreduce.Mapper.Context context
        ) throws IOException, InterruptedException {

            String result = value.toString().split(";")[1];
            /* we go through the gender field : if there is a m, we create a ('m',1) , else, we create a ('m',0)
            if there is a 'f', we create ('f',1) else we create ('f',0)*/
            word.set("f");
            context.write(word, new IntWritable(result.contains("f") ? 1 : 0));
            word.set("m");
            context.write(word, new IntWritable(result.contains("m") ? 1 : 0));
        }
    }

    public static class IntMeanReducer
        /* Attention : The proportion returned for m is the probability for example that a first name is at least masculine (it explains what it does not adds up to 1.)*/
            extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        private DoubleWritable result =new DoubleWritable();
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            /* we do the mean over the values : sum/count*/
            int sum = 0;
            int count=0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            result.set((double) sum / count);
            context.write(key, result);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "Task3");
        job.setJarByClass(Task3.class);
        job.setMapperClass(Mapper3.class);
        job.setReducerClass(IntMeanReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.out.println("Task3");
        System.out.println(args[0]);
        System.out.println(new Path(args[0]));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}