import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PRPreProcess {
    public static enum NodeCounter {
        COUNT
    };

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text> {
        private Text first = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            int counter = 1;
            while (itr.hasMoreTokens()) {
                if (counter >= 3) {
                    counter = 1;
                    continue;
                } else if (counter == 1) {
                    first.set(itr.nextToken());
                } else {
                    context.write(first, itr.nextToken());
                }
                counter += 1;
            }
        }
    }

    public static class Adjacency_list_generator
            extends Reducer<Text, Text, Text, PRNodeWritable> {

        public void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            context.getCounter(NodeCounter.COUNT).increment(1);
            int size = 0;
            for (Text val : values) {
                size += 1;
            }
            List<Text> temp = new ArrayList<Text>(size);
            for (Text val : values) {
                temp.add(val);
            }
            context.write(key, new PRNodeWritable(key, 0, temp));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "preprocess");
        job.setJarByClass(PRPreProcess.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(Adjacency_list_generator.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PRNodeWritable.class);
        FileInputFormat.addInputPath(job, "/user/hadoop/tmpout");
        FileOutputFormat.setOutputPath(job, new Path(args[4]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
