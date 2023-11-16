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

public class PRAdjust {
    public static class PRADMapper
            extends Mapper<Text, PRNodeWritable, Text, PRNodeWritable> {

        public void map(Text key, PRNodeWritable node, Context context) throws IOException, InterruptedException {
            long g = PageRank.node_count;
            double a = PageRank.alpha;
            double m = 1 - PageRank.PRReducer.Mcount;
            if (g <= 0) {
                System.exit(-5);
            }
            double p_star = a / g + (1 - a) * (m / g + node.get_PR_value());
            node.set_PR_value(p_star);
            context.write(key, node);
        }
    }

    public static class PRADReducer
            extends Reducer<Text, PRNodeWritable, Text, PRNodeWritable> {

        public void reduce(Text key, PRNodeWritable node, Context context) throws IOException, InterruptedException {
            context.write(key, node);
        }
    }
}
