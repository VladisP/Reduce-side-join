package lab2;

import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class AirportIdPartitioner<K,V> extends HashPartitioner<K,V> {

    @Override
    public int getPartition(K key, V value, int numReduceTasks) {

        return ;
    }
}
