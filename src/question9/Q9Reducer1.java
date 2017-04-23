package question9;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Q9Reducer1 extends Reducer<Text, Text, Text, Text>{
		
		private Text vector = new Text();
		
        public void reduce(Text key, Iterable<Text> values, Context context) throws
        IOException, InterruptedException { 
        	
        	int[] sums = new int[34];
        	
        	for (Text val : values) {
        		String[] fields = val.toString().split("<===>");
        		for (int i = 0; i < fields.length; i++) {
        			sums[i] += Integer.parseInt(fields[i]);
        		}
        	}
        	
        	String csvValues = "";
        	
        	for (int i = 0; i < sums.length; i++) {
        		csvValues += Integer.toString(sums[i]) + ",";
        	}
        	
        	vector.set(csvValues);
        	context.write(key, vector);
        	
               	
        }
}