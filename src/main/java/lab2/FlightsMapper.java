package lab2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FlightsMapper extends Mapper<LongWritable, Text, AirportsIdWritable, Text> {


}
