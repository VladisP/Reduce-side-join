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
        destAirportId = Integer.parseInt(columns[14].replaceAll("\"", ""));
        String potentialDelayTime = columns[17].replaceAll("\"", "");
        delayTime = potentialDelayTime.equals("") ? null :
                Float.parseFloat(potentialDelayTime) > 0 ? null :
                        potentialDelayTime.replaceAll("-", "");
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
