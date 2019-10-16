package lab2.writables;

import lab2.CsvParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportTableWritable implements Writable {

    private int airportId;
    private String airportName;

    private static final int AIRPORT_ID_COLUMN = 0;
    private static final int AIRPORT_NAME_COLUMN = 1;

    public AirportTableWritable(Text text) {
//        String[] columns = text.toString().replaceAll("\"", "").split(",");

//        String potentialAirportId = columns[0];

    }

    public int getAirportId() {
        return airportId;
    }

    public String getAirportName() {
        return airportName;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(airportId);
        dataOutput.writeUTF(airportName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        airportId = dataInput.readInt();
        airportName = dataInput.readUTF();
    }
}
