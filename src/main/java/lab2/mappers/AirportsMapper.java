package lab2.mappers;

import lab2.CsvParser;
import lab2.writables.KeyDatasetPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportsMapper extends Mapper<LongWritable, Text, KeyDatasetPair, Text> {

    private static final int AIRPORT_ID_COLUMN = 0;
    private static final int AIRPORT_NAME_COLUMN = 1;
    private static final String FIRST_COLUMN_NAME = "Code";

    private int getAirportId(String[] columns) {
        String potentialAirportId = columns[AIRPORT_ID_COLUMN];

        return potentialAirportId.equals(FIRST_COLUMN_NAME) ? -1 : Integer.parseInt(potentialAirportId);
    }

    private String getAirportName(String[] columns) {
        return columns.length == 3 ?
                columns[AIRPORT_NAME_COLUMN] + columns[AIRPORT_NAME_COLUMN + 1] :
                columns[AIRPORT_NAME_COLUMN];
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] columns = CsvParser.getColumns(value);
        int airportId = getAirportId(columns);

        if (airportId != -1) {

            context.write(new KeyDatasetPair(airportId, 0),
                    new Text(getAirportName(columns)));
        }
    }
}
