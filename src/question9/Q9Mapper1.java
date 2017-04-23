package question9;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q9Mapper1 extends Mapper<LongWritable, Text, Text, Text>{

		private static Text state = new Text();
		private static Text ageData = new Text();
		
        public void map(LongWritable key, Text value, Context context) throws
                IOException, InterruptedException{
        	
        	String recordString = value.toString();
        	  	
        	String summaryLevel = recordString.substring(10, 13);
    		String logicalRecordPart = recordString.substring(24, 28);
    		
    		if (! (Integer.parseInt(summaryLevel) == 100)) return;
    		
    		if (Integer.parseInt(logicalRecordPart) == 2) return;
    		
    		String stateAbbreviation = recordString.substring(8, 10);
    		
    		state.set(stateAbbreviation);
    		
    		String finalPopulationInfo = "";
    		
    		String delimeter = "<===>";
    		
    		String totalPopulation = recordString.substring(300, 309);
    		String malePopulation = recordString.substring(363, 372);
			String femalePopulation = recordString.substring(372, 381);
			
			finalPopulationInfo = totalPopulation + delimeter + malePopulation + delimeter + femalePopulation + delimeter;
			
			int fieldStartLocation = 795;
			int fieldEndLocation = 804;
			String ageRangesPopulations = "";
			
			for (int i = 0; i < 31; i++) {
				ageRangesPopulations += (recordString.substring(fieldStartLocation, fieldEndLocation) + delimeter);
				fieldStartLocation = fieldEndLocation;
				fieldEndLocation += 9;
			}
			
			finalPopulationInfo += ageRangesPopulations;
			
			ageData.set(finalPopulationInfo);
			
			context.write(state, ageData);
			
			
        }
}