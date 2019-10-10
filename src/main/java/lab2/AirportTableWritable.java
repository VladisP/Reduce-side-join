package lab2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportTableWritable implements Writable {

    private String airportId;
    private String airportName;

    public AirportTableWritable(Text text) {

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF();
        dataOutput.writeBytes(airportId);
        dataOutput.writeBytes(airportName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        airportName = dataInput.readLine();
    }
}
