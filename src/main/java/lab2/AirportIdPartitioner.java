package lab2;

import lab2.writables.AirportsIdWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class AirportIdPartitioner extends HashPartitioner<AirportsIdWritable, Text> {

    @Override
    public int getPartition(AirportsIdWritable key, Text value, int numReduceTasks) {

        return key.getAirportId();
    }
}
