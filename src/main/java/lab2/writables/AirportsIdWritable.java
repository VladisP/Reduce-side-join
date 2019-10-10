package lab2.writables;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportsIdWritable implements WritableComparable<AirportsIdWritable> {

    private int airportId;
    private int dataSetId;

    public AirportsIdWritable(int airportId, int dataSetId) {
        this.airportId = airportId;
        this.dataSetId = dataSetId;
    }

    @Override
    public int compareTo(AirportsIdWritable o) {

        return (airportId < o.airportId) ? -1 :
                (airportId > o.airportId) ? 1 :
                        Integer.compare(dataSetId, o.dataSetId);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(airportId);
        dataOutput.writeInt(dataSetId);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        airportId = dataInput.readInt();
        dataSetId = dataInput.readInt();
    }
}
