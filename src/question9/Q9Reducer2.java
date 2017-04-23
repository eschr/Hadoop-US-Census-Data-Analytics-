package question9;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q9Reducer2 extends Reducer<NullWritable, Text, Text, Text>{
		
		private Text state = new Text();
		private Text topFiveCosSimList = new Text();
		
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws
        IOException, InterruptedException { 
        	
        	List<String> statesData = new ArrayList<String>();
        	
        	for (Text state : values) {
        		statesData.add(state.toString());
        	}
        	
        	String stateAbbreviationOne = "";
        	String stateAbbreviationTwo = "";
        	HashMap<String, Double> cosSimulatritiesBetweenStates;
        	
        	for (String stateStringOne : statesData) {
        		String[] abbreviationAndDataOne = stateStringOne.split("\\s+");
        		stateAbbreviationOne = abbreviationAndDataOne[0].trim();
        		
        		cosSimulatritiesBetweenStates = new HashMap<String, Double>();
        		for (String stateStringTwo : statesData) {
        			String[] abbreviationAndDataTwo = stateStringTwo.split("\\s+");
            		stateAbbreviationTwo = abbreviationAndDataTwo[0].trim();
            		
        			if (! stateAbbreviationOne.equals(stateAbbreviationTwo)) {
        				cosSimulatritiesBetweenStates.put(stateAbbreviationTwo, calcCosSim(abbreviationAndDataOne[1], abbreviationAndDataTwo[1]));
        			}
        		}
        		
        		String topFiveMostSimilar = getTopFiveStates(cosSimulatritiesBetweenStates);
        		
        		state.set(stateAbbreviationOne);
        		topFiveCosSimList.set(topFiveMostSimilar);
        		context.write(state, topFiveCosSimList);
        	}
               	
        }
        
        private String getTopFiveStates(HashMap<String, Double> cosSimulatritiesBetweenStates) {
			
        	Comparator<Entry<String, Double>> cosComparator = new Comparator<Entry<String, Double>>() {

				@Override
				public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
					Double cos1 = o1.getValue();
					Double cos2 = o2.getValue();
					return cos2.compareTo(cos1);
				}
        		
        	};
        	
        	Set<Entry<String, Double>> entrySet = cosSimulatritiesBetweenStates.entrySet();
        	List<Entry<String, Double>> entryList = new ArrayList<Entry<String, Double>>(entrySet);
        	
        	Collections.sort(entryList, cosComparator);
        	
        	String topFiveStates = "";
        	for (int i = 0; i < 5; i++) {
        		String stateAbbreviation = entryList.get(i).getKey();
        		String cosSim = Double.toString(entryList.get(i).getValue());
        		if (i == 0) topFiveStates += stateAbbreviation + ": cosine similarity = " + cosSim + "\n";
        		else topFiveStates += "\t" + stateAbbreviation + ": cosine similarity = " + cosSim + "\n";
        	}
        	
        	return topFiveStates;
		}

		private double calcCosSim(String stateOnesData, String stateTwosData) {
        	String[] stateOnePopData = stateOnesData.split(",");
        	String[] stateTwoPopData = stateTwosData.split(",");
        	
        	double stateOneTotalPopulation = Double.parseDouble(stateOnePopData[0]);
        	double stateTwoTotalPopulation = Double.parseDouble(stateTwoPopData[0]);
        	
        	int dataPoints = stateOnePopData.length;
        	
        	double dotProduct = 0;
        	double squaredA = 0;
        	double squaredB = 0;
        	
        	for (int i = 1; i < dataPoints; i++) {
        		double A = Integer.parseInt(stateOnePopData[i]) / stateOneTotalPopulation;
        		double B = Integer.parseInt(stateTwoPopData[i]) / stateTwoTotalPopulation;
        		
        		dotProduct += A * B;
        		squaredA += A * A;
        		squaredB += B * B;
        		
        	}
        	
        	
        return (1.0 * dotProduct) / ((Math.sqrt(squaredA)) * (Math.sqrt(squaredB)));
        	
        }
}