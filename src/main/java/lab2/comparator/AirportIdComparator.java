package lab2.comparator;

import lab2.writables.AirportsIdWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AirportIdComparator extends WritableComparator {

    protected AirportIdComparator() {
        
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        int firstAirportId = ((AirportsIdWritable) a).getAirportId();
        int secondAirportId = ((AirportsIdWritable) b).getAirportId();

        return Integer.compare(firstAirportId, secondAirportId);
    }
}
