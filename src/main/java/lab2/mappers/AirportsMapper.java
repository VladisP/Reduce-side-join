package lab2.mappers;

import lab2.writables.AirportTableWritable;
import lab2.writables.AirportsIdWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportsMapper extends Mapper<LongWritable, Text, AirportsIdWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        AirportTableWritable airportTable = new AirportTableWritable(value);

        if (airportTable.getAirportId() != -1) {

            context.write(new AirportsIdWritable(airportTable.getAirportId(), 0),
                    new Text(airportTable.getAirportName()));
        }
    }
}
