package lab2.partitioner;

import lab2.writables.KeyDatasetPair;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class AirportIdPartitioner extends HashPartitioner<KeyDatasetPair, Text> {

    @Override
    public int getPartition(KeyDatasetPair key, Text value, int numReduceTasks) {

        return (Integer.valueOf(key.getAirportId()).hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
}
