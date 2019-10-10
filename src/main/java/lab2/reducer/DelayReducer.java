package lab2.reducer;

import lab2.writables.AirportsIdWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DelayReducer extends Reducer<AirportsIdWritable, Text, Text, Text> {

    @Override
    protected void reduce(AirportsIdWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        
    }
}
