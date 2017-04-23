package pkg;
import java.io.IOException;
import java.security.acl.Owner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StateMapper extends Mapper<LongWritable, Text, Text, MapWritable>{

        private static Text state = new Text();
        private static MapWritable dataMap;

        public void map(LongWritable key, Text value, Context context) throws
                IOException, InterruptedException{
        	
        		dataMap = new MapWritable();
        		
        		String recordString = value.toString();
        		
        		String summaryLevel = recordString.substring(10, 13);
        		String logicalRecordPart = recordString.substring(24, 28);
        		
        		dataMap.put(new Text("recordPart"), new Text(logicalRecordPart));
        	    
                // Check that summary level is equal to 100 
        		if (! (Integer.parseInt(summaryLevel) == 100)) return;
        		
        		
        		if (Integer.parseInt(logicalRecordPart) == 2) {
        			
        			// Fields for Question 1
        			pullQuestionOneDataAddToMap(recordString);
        			
        			// Fields for Question 4
        			pullQuestionFourDataAddToMap(recordString);
        			
        			// Fields for Question 5 
        			pullQuestionFiveDataAddToMap(recordString);
        			
        			// Fields for Question 6
        			pullQuestionSixDataAddToMap(recordString);
        			
        		}
        		else {
        			// Fields for Question 2:
        			pullQuestionTwoDataAddToMap(recordString);	
        			
        			// Fields for Question 3
        			pullQuestionThreeDataAddToMap(recordString);
        			
        		}
        		
        		String stateAbbreviation = recordString.substring(8, 10);
        		
        		state.set(stateAbbreviation);
        		
        		context.write(state, dataMap);
        	
        }
        

		private void pullQuestionOneDataAddToMap(String recordString) {
        	String ownedHomes = recordString.substring(1803, 1812);
			String rentedHomes = recordString.substring(1812, 1821);
			String finalQ1 = ownedHomes + "<===>" + rentedHomes;
			dataMap.put(new Text("Q1"), new Text(finalQ1));
        }
        
        private void pullQuestionTwoDataAddToMap(String recordString) {
        	String malePopulation = recordString.substring(363, 372);
			String femalePopulation = recordString.substring(372, 381);
			String maleNeverMarried = recordString.substring(4422, 4431);
			String femaleNeverMarried = recordString.substring(4467, 4476);
			String finalQ2 = malePopulation + "<===>" + maleNeverMarried + "<===>" + femalePopulation + "<===>" + femaleNeverMarried;
			dataMap.put(new Text("Q2"), new Text(finalQ2));	
        }
        
        private void pullQuestionThreeDataAddToMap(String recordString) {
        	String hispanicMalePopulation = getHispanicMalePop(recordString);
        	String hispanicMales18AndUnder = getHispanicMalesUnder18(recordString);
        	String hispanicMales19To29 = getHispanicMales19To29(recordString);
        	String hispanicMales30To39 = getHispanicMales30To39(recordString);
        	String hispanicFemalePop = getHispanicFemalePop(recordString);
        	String hispanicFemales18AndUnder = getHispanicFemalesUnder18(recordString);
        	String hispanicFemales19To29 = getHispanicFemales19To29(recordString);
        	String hispanicFemales30To39 = getHispanicFemales30To39(recordString);
        	
        	String finalQ3 = hispanicMalePopulation + "<===>" + hispanicMales18AndUnder + "<===>" + hispanicMales19To29 + "<===>" + hispanicMales30To39 
        			+ "<===>" + hispanicFemalePop + "<===>" + hispanicFemales18AndUnder + "<===>" + hispanicFemales19To29 + "<===>" + hispanicFemales30To39;
        	
        	dataMap.put(new Text("Q3"), new Text(finalQ3));
        }
        
        private void pullQuestionFourDataAddToMap(String recordString) {
        	String urbanHouseInside = recordString.substring(1821, 1830);
			String urbanHouseOutside = recordString.substring(1830, 1839);
			String ruralHouse = recordString.substring(1839, 1848);
			String undefinedHouses = recordString.substring(1848, 1857);
			String finalQ4 = urbanHouseInside + "<===>" + urbanHouseOutside + "<===>" + ruralHouse + "<===>"
				+ undefinedHouses;
			dataMap.put(new Text("Q4"), new Text(finalQ4));
        }
        
        private void pullQuestionFiveDataAddToMap(String recordString) {
        	int fieldStartLocation = 2928;
			int fieldEndLocation = 2937;
			String homeValues = "";
			
			for (int i = 0; i < 20; i++) {
				homeValues += (recordString.substring(fieldStartLocation, fieldEndLocation) + "<===>");
				fieldStartLocation = fieldEndLocation;
				fieldEndLocation += 9;
			}
			
			dataMap.put(new Text("Q5"), new Text(homeValues));
        }
        
        private void pullQuestionSixDataAddToMap(String recordString) {
			int fieldStartLocation = 3450;
			int fieldEndLocation = 3459;
			String rentValues = "";
			
			for (int i = 0; i < 16; i++) {
				rentValues += (recordString.substring(fieldStartLocation, fieldEndLocation) + "<===>");
				fieldStartLocation = fieldEndLocation;
				fieldEndLocation += 9;
			}
			
			dataMap.put(new Text("Q6"), new Text(rentValues));
		}
        
        
        private String getHispanicFemalePop(String recordString) {
        	int hispanicFemaleCount = 0;
			
			int fieldStartLocation = 4143;
			int fieldEndLocation = 4152;
			
			for (int i = 0; i < 31; i++) {
				hispanicFemaleCount += Integer.parseInt(recordString.substring(fieldStartLocation, fieldEndLocation));
				fieldStartLocation = fieldEndLocation;
				fieldEndLocation += 9;
			}
			
			return Integer.toString(hispanicFemaleCount);
		}

		private String getHispanicMalePop(String recordString) {
        	int hispanicMaleCount = 0;
			
			int fieldStartLocation = 3864;
			int fieldEndLocation = 3873;
			
			for (int i = 0; i < 31; i++) {
				hispanicMaleCount += Integer.parseInt(recordString.substring(fieldStartLocation, fieldEndLocation));
				fieldStartLocation = fieldEndLocation;
				fieldEndLocation += 9;
			}
			
			return Integer.toString(hispanicMaleCount);
		}

		private String getHispanicMalesUnder18(String recordString) {
			int under18Count = 0;
			
			int fieldStartLocation = 3864;
			int fieldEndLocation = 3873;
			
			for (int i = 0; i < 13; i++) {
				under18Count += Integer.parseInt(recordString.substring(fieldStartLocation, fieldEndLocation));
				fieldStartLocation = fieldEndLocation;
				fieldEndLocation += 9;
			}
			
			return Integer.toString(under18Count);
		}

		private String getHispanicMales30To39(String recordString) {
			int thirtyToThirtyFour = Integer.parseInt(recordString.substring(4026, 4035));
			int thirtyFiveToThirtyNine = Integer.parseInt(recordString.substring(4035, 4044));
			return Integer.toString(thirtyToThirtyFour + thirtyFiveToThirtyNine);
		}

		private String getHispanicMales19To29(String recordString) {
			int hispanicMale19To29Count = 0;
			
			int fieldStartLocation = 3981;
			int fieldEndLocation = 3990;
			
			for (int i = 0; i < 5; i++) {
				hispanicMale19To29Count += Integer.parseInt(recordString.substring(fieldStartLocation, fieldEndLocation));
				fieldStartLocation =  fieldEndLocation;
				fieldEndLocation += 9;
			}
			
			return Integer.toString(hispanicMale19To29Count);
		}
		
		private String getHispanicFemalesUnder18(String recordString) {
			int under18Count = 0;

			int fieldStartLocation = 4143;
			int fieldEndLocation = 4152;

			for (int i = 0; i < 13; i++) {
				under18Count += Integer.parseInt(recordString.substring(fieldStartLocation, fieldEndLocation));
				fieldStartLocation = fieldEndLocation;
				fieldEndLocation += 9;
			}

			return Integer.toString(under18Count);
		}

		private String getHispanicFemales30To39(String recordString) {
			int thirtyToThirtyFour = Integer.parseInt(recordString.substring(4305, 4314));
			int thirtyFiveToThirtyNine = Integer.parseInt(recordString.substring(4314, 4323));
			return Integer.toString(thirtyToThirtyFour + thirtyFiveToThirtyNine);
		}

		private String getHispanicFemales19To29(String recordString) {
			int hispanicFemale19To29Count = 0;
			
			int fieldStartLocation = 4260;
			int fieldEndLocation = 4269;
			
			for (int i = 0; i < 5; i++) {
				hispanicFemale19To29Count += Integer.parseInt(recordString.substring(fieldStartLocation, fieldEndLocation));
				fieldStartLocation =  fieldEndLocation;
				fieldEndLocation += 9;
			}
			
			return Integer.toString(hispanicFemale19To29Count);
		}

}