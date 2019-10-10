package lab2.writables;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class FlightTableWritable implements Writable {

    private int destAirportId;
    private String delayTime;

    public FlightTableWritable(Text text) {
        String[] columns = text.toString().replaceAll(",", ",\"").split(",");

        String potentialDestAirportId = columns[14].replaceAll("\"", "");

        destAirportId = potentialDestAirportId.equals("DEST_AIRPORT_ID") ? -1 :
                Integer.parseInt(potentialDestAirportId);

        String potentialDelayTime = columns[17].replaceAll("\"", "");

        delayTime = potentialDelayTime.equals("ARR_DELAY") ? null :
                potentialDelayTime.equals("") ? null :
                        Float.parseFloat(potentialDelayTime) > 0 ? null :
                                potentialDelayTime.replaceAll("-", "");

        System.out.println(delayTime);
    }

    public int getDestAirportId() {
        return destAirportId;
    }

    public String getDelayTime() {
        return delayTime;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(destAirportId);
        dataOutput.writeUTF(delayTime);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        destAirportId = dataInput.readInt();
        delayTime = dataInput.readUTF();
    }
}
