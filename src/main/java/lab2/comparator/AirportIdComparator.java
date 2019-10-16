package lab2.comparator;

import lab2.writables.KeyDatasetPair;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AirportIdComparator extends WritableComparator {

    protected AirportIdComparator() {
        super(KeyDatasetPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        int firstAirportId = ((KeyDatasetPair) a).getAirportId();
        int secondAirportId = ((KeyDatasetPair) b).getAirportId();

        return Integer.compare(firstAirportId, secondAirportId);
    }
}
