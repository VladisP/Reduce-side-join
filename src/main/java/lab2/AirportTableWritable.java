package lab2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportTableWritable implements Writable {

    private int airportId;
    private String airportName;

    public AirportTableWritable(Text text) {
        String[] columns = text.toString().replaceAll("\"", "").split(",");
        airportId = Integer.parseInt(columns[0]);
        airportName = columns[1];
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
