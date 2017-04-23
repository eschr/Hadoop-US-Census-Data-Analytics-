package questions7And8;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q7_Q8Reducer2 extends Reducer<NullWritable, Text, Text, Text>{
		
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws
        IOException, InterruptedException { 
        	
        	ArrayList<Double> avgNumberOfRooms = new ArrayList<Double>();
        	String stateWithMostSeniors = "";
        	double maxSeniors = 0.0;
        	// STATE, >85 %, AVG # ROOMS
        	for (Text val : values) {
        		String[] fields = val.toString().split("\\s+");
        		avgNumberOfRooms.add(Double.parseDouble(fields[2]));
        		
        		double seniorsPercent = Double.parseDouble(fields[1]);
        		if (seniorsPercent > maxSeniors) {
        			stateWithMostSeniors = fields[0];
        			maxSeniors = seniorsPercent;
        		}
        	}
        	
        	Collections.sort(avgNumberOfRooms);
        	int percentileIndex = (int) Math.ceil(0.95 * avgNumberOfRooms.size()) - 1;
        	
        	double ninetyFifthPercentile = avgNumberOfRooms.get(percentileIndex);
        	
        	context.write(new Text("95th percentile of avg number of rooms:"), new Text(Double.toString(ninetyFifthPercentile)));
        	context.write(new Text(stateWithMostSeniors), new Text(Double.toString(maxSeniors) + "% > 85"));
        	
               	
        }
}