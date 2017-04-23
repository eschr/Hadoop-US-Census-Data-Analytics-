package questions7And8;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Q7_Q8Mapper2 extends Mapper<LongWritable, Text, NullWritable, Text>{

        public void map(LongWritable key, Text value, Context context) throws
                IOException, InterruptedException{
        	  	
        	context.write(NullWritable.get(), value);
        }
}