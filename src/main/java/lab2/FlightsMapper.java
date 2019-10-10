package lab2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Optional;
import java.util.function.Consumer;

public class FlightsMapper extends Mapper<LongWritable, Text, AirportsIdWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        FlightTableWritable flightTable = new FlightTableWritable(value);

        Optional.ofNullable(flightTable.getDelayTime()).ifPresent(new Consumer<String>() {
            @Override
            public void accept(String s) {
                context.write(new AirportsIdWritable(flightTable.getDestAirportId(), 1));
            }
        });
    }
}
