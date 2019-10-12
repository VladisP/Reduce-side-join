package lab2.mappers;

import lab2.CsvParser;
import lab2.writables.AirportsIdWritable;
import lab2.writables.FlightTableWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightsMapper extends Mapper<LongWritable, Text, AirportsIdWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] columns = CsvParser.getColumns(text, true);
        String potentialDestAirportId = CsvParser.getColumn(columns, DEST_AIRPORT_ID_COLUMN);

        if ((flightTable.getDestAirportId() != -1) && (flightTable.getDelayTime() != null)) {

            context.write(new AirportsIdWritable(flightTable.getDestAirportId(), 1),
                    new Text(flightTable.getDelayTime()));
        }
    }
}
