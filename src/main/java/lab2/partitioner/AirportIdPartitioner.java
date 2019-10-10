package lab2.partitioner;

import lab2.writables.AirportsIdWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class AirportIdPartitioner extends HashPartitioner<AirportsIdWritable, Text> {

    @Override
    public int getPartition(AirportsIdWritable key, Text value, int numReduceTasks) {

        System.out.println(key.getAirportId() + " " + key.getDataSetId());

        int partition = (Integer.valueOf(key.getAirportId()).hashCode() & Integer.MAX_VALUE) % numReduceTasks;

        return partition;
    }
}
