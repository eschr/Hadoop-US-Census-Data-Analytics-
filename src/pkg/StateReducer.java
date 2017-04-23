package pkg;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StateReducer extends Reducer<Text, MapWritable, Text, Text>{
		
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws
        IOException, InterruptedException {
        	
                int owned = 0;
                int rented = 0;
                
                int malePop = 0;
                int femalePop = 0;
                int neverMarriedMales = 0;
                int neverMarriedFemales = 0;
                
                int urbanHouseholds = 0;
                int ruralHouseholds = 0;
                int undefinedHouseholds = 0;
                
                int hispanicMalePop = 0;
                int hispanicMaleUnder18 = 0;
                int hispanicMale19To29 = 0;
                int hispanicMale30To39 = 0;
                int hispanicFemalePop = 0;
                int hispanicFemaleUnder18 = 0;
                int hispanicFemale19To29 = 0;
                int hispanicFemale30To39 = 0;
                
                int[] rentFreq = new int[16];
                int sumOfRentFreq = 0;
                
                int[] homeValueFreq = new int[20];
                int sumofHomeValueFreq = 0;
                
                for(MapWritable val: values) {
                		String recordPart = val.get(new Text("recordPart")).toString();
                		
                		if (Integer.parseInt(recordPart) == 1) {
                			String[] q2Fields = val.get(new Text("Q2")).toString().split("<===>");
                			malePop += Integer.parseInt(q2Fields[0]);
                			neverMarriedMales += Integer.parseInt(q2Fields[1]);
                			femalePop += Integer.parseInt(q2Fields[2]);
                			neverMarriedFemales += Integer.parseInt(q2Fields[3]);
                			
                			String[] q3Fields = val.get(new Text("Q3")).toString().split("<===>");
                			 hispanicMalePop += Integer.parseInt(q3Fields[0]);
                             hispanicMaleUnder18 += Integer.parseInt(q3Fields[1]);
                             hispanicMale19To29 += Integer.parseInt(q3Fields[2]);
                             hispanicMale30To39 += Integer.parseInt(q3Fields[3]);
                             hispanicFemalePop += Integer.parseInt(q3Fields[4]);
                             hispanicFemaleUnder18 += Integer.parseInt(q3Fields[5]);
                             hispanicFemale19To29 += Integer.parseInt(q3Fields[6]);
                             hispanicFemale30To39 += Integer.parseInt(q3Fields[7]);
                			
                		}
                		else {
                			String[] q1Fields = val.get(new Text("Q1")).toString().split("<===>");
                			owned += Integer.parseInt(q1Fields[0]);
                			rented += Integer.parseInt(q1Fields[1]);
                			
                			String[] q4Fields = val.get(new Text("Q4")).toString().split("<===>");
                			urbanHouseholds += (Integer.parseInt(q4Fields[0]));
                			urbanHouseholds += (Integer.parseInt(q4Fields[1]));
                			ruralHouseholds += (Integer.parseInt(q4Fields[2]));
                			undefinedHouseholds += (Integer.parseInt(q4Fields[3]));
                			
                			String[] q6Fields = val.get(new Text("Q6")).toString().split("<===>");
                			for (int i = 0; i < 16; i++) {
                				int temp = Integer.parseInt(q6Fields[i]);
                				rentFreq[i] = rentFreq[i] + temp;
                				sumOfRentFreq += temp;
                			}
                			
                			String[] q5Fields = val.get(new Text("Q5")).toString().split("<===>");
                			for (int i = 0; i < 20; i++) {
                				int temp = Integer.parseInt(q5Fields[i]);
                				homeValueFreq[i] = homeValueFreq[i] + temp;
                				sumofHomeValueFreq += temp;
                			}
                		}
                }
                
                // RESULTS Q1
                String q1 = calcOwnedAndRentedResidences(owned, rented);
                context.write(key, new Text(q1));
                
                // RESULTS Q2
                String q2 = calcNeverMarried(neverMarriedMales, malePop, neverMarriedFemales, femalePop);
                context.write(key, new Text(q2));
                
                // RESULTS Q3
                String q3 = calcHispanicAgeGenderDistribution(hispanicMalePop, hispanicMaleUnder18, hispanicMale19To29, hispanicMale30To39, hispanicFemalePop,
                		hispanicFemaleUnder18, hispanicFemale19To29, hispanicFemale30To39);
                context.write(key, new Text(q3));
                
                // RESULTS Q4
                String q4 = calcUrbanRuralHouseholds(urbanHouseholds, ruralHouseholds, undefinedHouseholds);    
                context.write(key, new Text(q4));
                
                String q5 = calcMedianHouseValues(homeValueFreq, sumofHomeValueFreq);
                context.write(key, new Text(q5));
                
                String q6 = calcMedianRentFreq(rentFreq, sumOfRentFreq);
                context.write(key, new Text(q6));       
             
        }
        
        private String calcMedianHouseValues(int[] homeValueFreq, int sumofHomeValueFreq) {
			int mid = sumofHomeValueFreq / 2;
			int current = 0;
			int i;
			for (i = 0; i < 20; i++) {
				current += homeValueFreq[i];
				if (current >= mid) break;
			}
			
			return getHouseValueRange(i);
		}

		private String getHouseValueRange(int i) {
			String homeValueRange = "";
			
			switch(i) {
				case 0: homeValueRange = "Less than $15,000";
					break;
				case 1: homeValueRange = "$15,000 - $19,999";
					break;
				case 2: homeValueRange = "$20,000 -  $24,999";
					break;
				case 3: homeValueRange = "$25,000 - $29,999";
					break;
				case 4: homeValueRange = "$30,000 - $34,999";
					break;
				case 5: homeValueRange = "$35,000 - $39,999";
					break;
				case 6: homeValueRange = "$40,000 - $44,999";
					break;
				case 7: homeValueRange = "$45,000 - $49,999";
					break;
				case 8: homeValueRange = "$50,000 - $59,999";
					break;
				case 9: homeValueRange = "$60,000 - $74,999";
					break;
				case 10: homeValueRange = "$75,000 - $99,999";
					break;
				case 11: homeValueRange = "$100,000 - $124,999";
					break;
				case 12: homeValueRange = "$125,000 - $149,999";
					break;
				case 13: homeValueRange = "$150,000 - $174,999";
					break;
				case 14: homeValueRange = "$175,000 - $199,999";
					break;
				case 15: homeValueRange = "$200,000 - $249,999";
					break;
				case 16: homeValueRange = "$250,000 - $299,999";
					break;
				case 17: homeValueRange = "$300,000 - $399,999";
					break;
				case 18: homeValueRange = "$400,000 - $499,999";
					break;
				case 19: homeValueRange = "$500,000 or more";
					break;
				default: homeValueRange = "Error";
					break;		
			}
			
			return homeValueRange;
		}

		private String calcMedianRentFreq(int[] rentFreq, int sumOfRentFreq) {
        	int mid = sumOfRentFreq / 2;
        	int current = 0;
        	int i;
        	for (i = 0; i < 16; i++) {
        		current += rentFreq[i];
        		if (current >= mid) break;
        	}
        	
        	return getRentRange(i);
        }

		private String getRentRange(int i) {
			
			String rent = "";
			
			switch (i) {
				case 0: rent = "Less than $100";
					break;
				case 1: rent = "$100 to $149";
					break;
				case 2: rent = "$150 to $199";
					break;
				case 3: rent = "$200 to $249";
					break;
				case 4: rent = "$250 to $299";
					break;
				case 5: rent = "$300 to $349";
					break;
				case 6: rent = "$350 to $399";
					break;
				case 7: rent = "$400 to $449";
					break;
				case 8: rent = "$450 to $499";
					break;
				case 9: rent = "$500 to $549";
					break;
				case 10: rent = "$550 to $599";
					break;
				case 11: rent = "$600 to $649";
					break;
				case 12: rent = "$650 to $699";
					break;
				case 13: rent = "$700 to $749";
					break;
				case 14: rent = "$750 to $999";
					break;
				case 15: rent = "$1000 or more";
					break;
				default: rent = "Error";
					break;
			}
			
			return rent + "\n";
		}

		private String calcHispanicAgeGenderDistribution(int hispanicMalePop, int hispanicMaleUnder18,
				int hispanicMale19To29, int hispanicMale30To39, int hispanicFemalePop, int hispanicFemaleUnder18,
				int hispanicFemale19To29, int hispanicFemale30To39) {
        	
        	String percentMalesUnder18 = String.format("%.2f", ((1.0 * hispanicMaleUnder18) / hispanicMalePop * 100));
        	String percentMales19To29 = String.format("%.2f", ((1.0 * hispanicMale19To29) / hispanicMalePop * 100));
        	String percentMales30To39 = String.format("%.2f", ((1.0 * hispanicMale30To39) / hispanicMalePop * 100));
        	
        	String percentFemalesUnder18 = String.format("%.2f", ((1.0 * hispanicFemaleUnder18) / hispanicFemalePop * 100));
        	String percentFemales19To29 = String.format("%.2f", ((1.0 * hispanicFemale19To29) / hispanicFemalePop * 100));
        	String percentFemales30To39 = String.format("%.2f", ((1.0 * hispanicFemale30To39) / hispanicFemalePop * 100));
        	
        	return "Hispanic M 00 - 18:" + "\t" + percentMalesUnder18 + "\n" +
        	"\t" + "Hispanic M 19 - 29:" + "\t" + percentMales19To29 + "\n" +
        	"\t" + "Hispanic M 30 - 39:" + "\t" + percentMales30To39 + "\n" +
        	"\t" + "Hispanic F 00 - 18:" + "\t" + percentFemalesUnder18 + "\n" +
        	"\t" + "Hispanic F 19 - 29:" + "\t" + percentFemales19To29 + "\n" +
        	"\t" + "Hispanic F 30 - 39:" + "\t" + percentFemales30To39;
  		}

		private String calcUrbanRuralHouseholds(int urbanHouseholds, int ruralHouseholds, int undefinedHouseholds) {
        	 int totalHouseholds = urbanHouseholds + ruralHouseholds + undefinedHouseholds;
             double urbanPercent = ((1.0 * urbanHouseholds) / totalHouseholds) * 100;
             double ruralPercent = ((1.0 * ruralHouseholds) / totalHouseholds) * 100;
             
             String q4 = "Urban Households: " + String.format("%.2f", urbanPercent) + "\n"
            		 + "\t" + "Rural Households: " + String.format("%.2f", ruralPercent);
             
             return q4;
		}

		private String calcNeverMarried(int neverMarriedMales, int malePop, int neverMarriedFemales, int femalePop) {
			 int totalPop = malePop + femalePop;
        	 double percentageMalesNeverMarried = ((1.0 * neverMarriedMales) / totalPop) * 100;
             double percentageFemalesNeverMarried = ((1.0 * neverMarriedFemales) / totalPop) * 100;       
             
             String q2 = "Males: " + malePop + " |> " + neverMarriedMales + " -- " + String.format("%.2f", percentageMalesNeverMarried) 
             		+ " Females: " + femalePop + " |> " + neverMarriedFemales + " -- " + String.format("%.2f", percentageFemalesNeverMarried);
             
             return q2;
		}

		private String calcOwnedAndRentedResidences(int owned, int rented) {
        	int totalResidences = owned + rented;
            double ownedResidencesPercent = ((1.0 * owned) / totalResidences) * 100;
            double rentedResidencesPercent = ((1.0 * rented) / totalResidences) * 100;
            
            String q1 = "Owned: " + String.format("%.2f", ownedResidencesPercent) + " Rented: " + String.format("%.2f", rentedResidencesPercent);
            
            return q1;
        }
}
