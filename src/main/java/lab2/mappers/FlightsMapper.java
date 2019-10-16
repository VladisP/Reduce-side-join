package lab2.mappers;

import lab2.CsvParser;
import lab2.writables.KeyDatasetPair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightsMapper extends Mapper<LongWritable, Text, KeyDatasetPair, Text> {

    private static final int DEST_AIRPORT_ID_COLUMN = 14;
    private static final int DELAY_TIME_COLUMN = 18;
    private static final String ID_HEAD_VALUE = "DEST_AIRPORT_ID";
    private static final String DELAY_HEAD_VALUE = "ARR_DELAY_NEW";

    private int getDestAirportId(String[] columns) {
        String potentialDestAirportId = columns[DEST_AIRPORT_ID_COLUMN];

        return potentialDestAirportId.equals(ID_HEAD_VALUE) ? -1 :
                Integer.parseInt(potentialDestAirportId);
    }

    private String getDelayTime(String[] columns) {
        String potentialDelayTime = columns[DELAY_TIME_COLUMN];

        if (potentialDelayTime.equals(DELAY_HEAD_VALUE) ||
                potentialDelayTime.equals("") ||
                Float.parseFloat(potentialDelayTime) == 0f) {
            return null;
        } else {
            return potentialDelayTime;
        }
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] columns = CsvParser.getColumns(value);
        int destAirportId = getDestAirportId(columns);
        String delayTime = getDelayTime(columns);

        if ((destAirportId != -1) && (delayTime != null)) {
            context.write(new KeyDatasetPair(destAirportId, 1), new Text(delayTime));
        }
    }
}
