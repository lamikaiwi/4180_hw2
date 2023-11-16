import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;

public class PRNodeWritable implements Writable {
    private Text id;
    private DoubleWritable PR_value;
    private List<Text> Adjacency_list;

    public PRNodeWritable() {
        id = new Text();
        PR_value = new DoubleWritable(0);
        Adjacency_list = new ArrayList<Text>();
    }

    public PRNodeWritable(Text id, DoubleWritable PR_value, List<Text> Adjacency_list) {
        this.id = id;
        this.PR_value = PR_value;
        this.Adjacency_list = Adjacency_list;
    }

    public void write(DataOutput out) throws IOException {
        id.write(out);
        PR_value.write(out);
        out.writeInt(Adjacency_list.size());
        for (int index = 0; index < Adjacency_list.size(); index++) {
            Adjacency_list.get(index).write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        id.readFields(in);
        PR_value.readFields(in);
        int size = in.readInt();
        Adjacency_list = new ArrayList<Text>(size);
        for (int index = 0; index < size; index++) {
            Text text = new Text();
            text.readFields(in);
            Adjacency_list.add(text);
        }
    }

    public PRNodeWritable read(DataInput in) throws IOException {
        PRNodeWritable node = new PRNodeWritable();
        node.readFields(in);
        return node;
    }

    @Override
    public String toString() {
        String output = "a " + id.toString() + " " + PR_value.toString() + " "
                + Integer.toString(Adjacency_list.size());
        for (Text adj : Adjacency_list) {
            output = output + " " + adj.toString();
        }
        return output;
    }

    public PRNodeWritable fromString(String s) throws IOException {
        StringTokenizer itr = new StringTokenizer(s, " ");
        String s = itr.nextToken();
        id.set(itr.nextToken());
        PR_value.set(Double.parseDouble(itr.nextToken()));
        int size = Integer.parseInt(itr.nextToken());
        Adjacency_list = new ArrayList<Text>(size);
        for (int index = 0; index < Adjacency_list.size(); index++) {
            Adjacency_list.add(itr.nextToken());
        }
    }

    public void addList(int n) {
        Adjacency_list.add(new IntWriteable(n));
    }

    public List<Text> get_Adjacency_List() {
        return Adjacency_list;
    }

    public Text get_id() {
        return id;
    }

    public DoubleWritable get_PR_value() {
        return p;
    }

    public void set_PR_value(double p) {
        PR_value.set(p);
    }
}
