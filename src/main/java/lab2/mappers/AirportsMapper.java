package lab2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportsMapper extends Mapper<LongWritable, Text, AirportsIdWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        AirportTableWritable airportTable = new AirportTableWritable(value);

        context.write(new AirportsIdWritable(airportTable.getAirportId(), 0),
                new Text(airportTable.getAirportName()));
    }
}
