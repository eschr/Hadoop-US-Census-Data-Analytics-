package questions7And8;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q7_Q8Reducer1 extends Reducer<Text, MapWritable, Text, Text>{
		
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws
        IOException, InterruptedException { 
        	
        	int greaterThan85Count = 0;
        	int statePopulation = 0;
        	
        	int[] numberOfRoomsFreq = new int[9];
        	
        	for (MapWritable map : values) {
        		
        		String recordPart = map.get(new Text("recordPart")).toString();
        		
        		if (Integer.parseInt(recordPart) == 1) {
        			String[] q8Fields = map.get(new Text("Q8")).toString().split("<===>");
        			statePopulation += Integer.parseInt(q8Fields[0]);
        			greaterThan85Count += Integer.parseInt(q8Fields[1]);
        		}
        		else {
        			String[] q7Fields = map.get(new Text("Q7")).toString().split("<===>");
        			for (int i = 0; i < 9; i++) {
        				int temp = Integer.parseInt(q7Fields[i]);
        				numberOfRoomsFreq[i] = numberOfRoomsFreq[i] + temp; 
        			}
        		}
        		
        	}
        	
        	String intermediateOut = constructStringFromSums(greaterThan85Count, statePopulation, numberOfRoomsFreq);
        	
        	context.write(key, new Text(intermediateOut));
        }

		private String constructStringFromSums(int greaterThan85Count, int statePopulation, int[] numberOfRoomsFreq) {
			String percentageOf85 = String.format("%.2f", ((1.0 * greaterThan85Count) / statePopulation) * 100);
			int roomFrequencies = 0;
			int totalNumberOfHouses = 0;
			
			for (int i = 0; i < 9; i++) {
				int temp = numberOfRoomsFreq[i];
				roomFrequencies += temp * (i+1);
				totalNumberOfHouses += temp;
			}
			double roomsPerHouse = (1.0 * roomFrequencies) / totalNumberOfHouses;
			String q7 = String.format("%.2f", roomsPerHouse);
			
			return percentageOf85 + "\t" + q7;
		}
}