package questions7And8;
import java.io.IOException;
import java.security.acl.Owner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q7_Q8Mapper1 extends Mapper<LongWritable, Text, Text, MapWritable>{

        private static Text state = new Text();
        private static MapWritable dataMap;

        public void map(LongWritable key, Text value, Context context) throws
                IOException, InterruptedException{
        	
        	dataMap = new MapWritable();
        	
        	String recordString = value.toString();
        	
        	String summaryLevel = recordString.substring(10, 13);
    		int logicalRecordPart = Integer.parseInt(recordString.substring(24, 28));
    		
    		if (! (Integer.parseInt(summaryLevel) == 100)) return; 
    		
        	String stateAbbreviation = recordString.substring(8, 10);
    		state.set(stateAbbreviation);
    		
    		dataMap.put(new Text("recordPart"), new Text(Integer.toString(logicalRecordPart)));
    		
    		if (logicalRecordPart == 2)
    			// Fields for question 7
    			pullAverageNumberOfRoomsAddToMap(recordString);
    		
    		if (logicalRecordPart == 1)
    			// Fields for question 8
    			pullGreaterThan85DataAddToMap(recordString);
    		
    		
    		context.write(state, dataMap);
        }

		private void pullGreaterThan85DataAddToMap(String recordString) {
			String totalPopulation = recordString.substring(300, 309);
			String greaterThan85 = recordString.substring(1065, 1074);
			String q8 = totalPopulation + "<===>" + greaterThan85;
			
			dataMap.put(new Text("Q8"), new Text(q8));
		}

		private void pullAverageNumberOfRoomsAddToMap(String recordString) {
			int fieldStartLocation = 2388;
			int fieldEndLocation = 2397;
			String numberOfHousesWith_i_Rooms = "";
			
			for (int i = 0; i < 9; i++) {
				numberOfHousesWith_i_Rooms += (recordString.substring(fieldStartLocation, fieldEndLocation) + "<===>");
				fieldStartLocation = fieldEndLocation;
				fieldEndLocation += 9;
			}
			
			dataMap.put(new Text("Q7"), new Text(numberOfHousesWith_i_Rooms));
			
		}
}

