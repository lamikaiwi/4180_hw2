import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;

public class OutputMapper {
    public static class outputmapper
            extends Mapper<Text, PRNodeWritable, Text, DoubleWritable> {

        public void map(Text key, PRNodeWritable node, Context context) throws IOException, InterruptedException {
            double t = PageRank.threshold;
            if (node.get_PR_value() >  t){
                context.write(key, node.get_PR_value());
            }
        }
    }

    public static class outputreducer
            extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

        public void reduce(Text key, DoubleWritable p, Context context) throws IOException, InterruptedException {
            context.write(key, p);
        }
    }
}
