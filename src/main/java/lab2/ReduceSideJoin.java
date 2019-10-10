package lab2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.IOException;

public class ReduceSideJoin {

    public static void main(String[] args) throws IOException {

        Job job = Job.getInstance();
        job.setJarByClass(ReduceSideJoin.class);
        job.setJobName("Reduce-side-join lab2");
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, );
    }
}
