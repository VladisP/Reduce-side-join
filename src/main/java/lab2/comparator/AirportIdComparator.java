package lab2.comparator;

import lab2.writables.AirportsIdWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class AirportIdComparator extends WritableComparator {

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return super.compare(b1, s1, l1, b2, s2, l2);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        int firstAirportId = ((AirportsIdWritable) a).getAirportId();
        int secondAirportId = ((AirportsIdWritable) b).getAirportId();

        return Integer.compare(firstAirportId, secondAirportId);
    }
}
