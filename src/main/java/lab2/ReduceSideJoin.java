package lab2;

import lab2.comparator.AirportIdComparator;
import lab2.mappers.AirportsMapper;
import lab2.mappers.FlightsMapper;
import lab2.partitioner.AirportIdPartitioner;
import lab2.reducer.DelayReducer;
import lab2.writables.KeyDatasetPair;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ReduceSideJoin {

    public static void main(String[] args) throws Exception {

        Job job = Job.getInstance();
        job.setJarByClass(ReduceSideJoin.class);
        job.setJobName("Reduce-side-join lab2");

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FlightsMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AirportsMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        job.setPartitionerClass(AirportIdPartitioner.class);
        job.setGroupingComparatorClass(AirportIdComparator.class);
        job.setReducerClass(DelayReducer.class);

        job.setMapOutputKeyClass(KeyDatasetPair.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(2);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
