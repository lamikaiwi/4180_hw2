import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import outputmapper.outputreducer;

import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class PageRank extends PRPreProcess {

    public static long node_count = 0;
    public static int iteration = 0;
    public static double alpha = 0, threshold = 0;

    public static class PRMapper
            extends Mapper<Text, PRNodeWritable, Text, Text> {
        private Text key = new Text();

        public void map(Text key, PRNodeWritable node, Context context) throws IOException, InterruptedException {
            if (node_count <= 0) {
                System.exit(-4);
            }
            double PR = 1 / node_count;
            if (node.get_PR_value() == 0) {
                node.set_PR_value(PR);
            }
            List<Text> temp = node.get_Adjacency_List();
            int size = temp.size();
            int p = node.get_PR_value() / size;
            String s = node.toString();
            context.write(key, new Text(s));
            for (int index = 0; index < size; index++) {
                context.write(temp.get(index), new Text(p + ""));
            }
        }
    }

    public static class PRReducer
            extends Reducer<Text, Text, Text, PRNodeWritable> {
        public static double Mcount = 0;

        public void reduce(Text key, Iterable<Text> values,
                Context context) throws IOException, InterruptedException {
            PRNodeWritable M = new PRNodeWritable();
            double s = 0;
            for (Text val : values) {
                String st = val.toString();
                if (st.startsWith("a")) {
                    M.fromString(st);
                } else {
                    s += Double.parseDouble(st);
                }
            }
            M.set_PR_value(s);
            Mcount += s;
            context.write(key, M);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        alpha = Double.parseDouble(args[0]);
        iteration = Integer.parseInt(args[1]);
        threshold = Double.parseDouble(args[2]);
        Job job1 = Job.getInstance(conf, "preprocess");
        job1.setJarByClass(PRPreProcess.class);
        job1.setMapperClass(PRPreProcess.TokenizerMapper.class);
        job1.setReducerClass(PRPreProcess.Adjacency_list_generator.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(PRNodeWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[3]));
        FileOutputFormat.setOutputPath(job1, "/user/hadoop/tmpout1"));
        node_count = job1.getCounters().findCounter(PRPreProcess.NodeCounter.COUNT).getValue();
        int count = 1;
        while (count <= iteration && job1.waitForCompletion()) {
            Job job2 = Job.getInstance(conf, "PR");
            job2.setJarByClass(PageRank.class);
            job2.setMapperClass(PRMapper.class);
            job2.setReducerClass(PRReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(PRNodeWritable.class);
            FileInputFormat.addInputPath(job2, "/user/hadoop/tmpout"+count);
            FileOutputFormat.setOutputPath(job2, "/user/hadoop/tmpout"+(count+1));
            Job job3 = Job.getInstance(conf, "adjust");
            job3.addDependingJob(job1);
            job3.setJarByClass(PRAdjust.class);
            job3.setMapperClass(PRAdjust.PRADMapper.class);
            job3.setReducerClass(PRAdjust.PRADReducer.class);
            job3.setOutputKeyClass(Text.class);
            job3.setOutputValueClass(PRNodeWritable.class);
            FileInputFormat.addInputPath(job3, "/user/hadoop/tmpout1"+(count+1));
            FileOutputFormat.setOutputPath(job3, "/user/hadoop/tmpout1"+(count+2));

            JobControl jc = new JobControl();
            jc.addJob(job2);
            jc.addJob(job3);
            job3.addDependingJob(job2);
            jc.run();
            count++;
        }
        Job job4 = Job.getInstance(conf, "output");
        job4.setJarByClass(OutputMapper.class);
        job4.setMapperClass(OutputMapper.outputmapper.class);
        job4.setReducerClass(OutputMapper.outputreducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job4, "/user/hadoop/tmpout"+(count+1));
        FileOutputFormat.setOutputPath(job4, new Path(args[4]));
        System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }
}
