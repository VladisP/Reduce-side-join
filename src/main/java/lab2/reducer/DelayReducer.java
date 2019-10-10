package lab2.reducer;

import lab2.writables.AirportsIdWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class DelayReducer extends Reducer<AirportsIdWritable, Text, Text, Text> {

    @Override
    protected void reduce(AirportsIdWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        Iterator<Text> iterator = values.iterator();
        Text airportName = iterator.next();

        float maxDelayTime = Float.MIN_VALUE;
        float minDelayTime = Float.MAX_VALUE;
        float sumDelayTime = 0f;
        int count = 0;

        while (iterator.hasNext()) {
            float delayTime = Float.parseFloat(iterator.next().toString());

            if (delayTime > maxDelayTime) {
                maxDelayTime = delayTime;
            }

            if (delayTime < minDelayTime) {
                minDelayTime = delayTime;
            }

            sumDelayTime += delayTime;
            count++;
        }

        if (count != 0) {
            String.join()
            String.valueOf(sumDelayTime / count) + String.valueOf()
            context.write(airportName, new Text());
        }
    }
}
