package lab2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlightTableWritable implements Writable {

    private int destAirportId;
    private String delayTime;

    public FlightTableWritable(Text text) {
        String[] columns = text.toString().replaceAll(",", "\"").split(",");
        destAirportId = Integer.parseInt(columns[14].replaceAll("\"",""));
        delayTime = columns[1];
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(airportId);
        dataOutput.writeUTF(delayTime);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        airportId = dataInput.readInt();
        delayTime = dataInput.readUTF();
    }
}
