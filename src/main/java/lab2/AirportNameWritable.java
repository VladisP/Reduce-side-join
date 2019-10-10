package lab2;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportNameWritable implements Writable {

    private String airportName;

    

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeBytes(airportName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        airportName = dataInput.readLine();
    }
}
