package lab2.mappers;

import lab2.AirportsIdWritable;
import lab2.FlightTableWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightsMapper extends Mapper<LongWritable, Text, AirportsIdWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        FlightTableWritable flightTable = new FlightTableWritable(value);

        if (flightTable.getDelayTime() != null) {

            context.write(new AirportsIdWritable(flightTable.getDestAirportId(), 1),
                    new Text(flightTable.getDelayTime()));
        }
    }
}
