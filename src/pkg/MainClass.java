package pkg;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import question9.Q9Mapper1;
import question9.Q9Mapper2;
import question9.Q9Reducer1;
import question9.Q9Reducer2;
import questions7And8.Q7_Q8Mapper1;
import questions7And8.Q7_Q8Mapper2;
import questions7And8.Q7_Q8Reducer1;
import questions7And8.Q7_Q8Reducer2;




public class MainClass {
	
	public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
		
		if (args.length != 2) {
			System.out.println("See usage: <input directory> <output directory>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
	
		Job job = Job.getInstance(conf, "job");
		
		job.setJarByClass(MainClass.class);
        job.setMapperClass(StateMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);
        job.setReducerClass(StateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
       
        
        Job job1 = Job.getInstance(conf, "job1");
        
        Path INTERMEDIATE_OUTPUT = new Path("/home/q7q8Intermediate");
		
		job1.setJarByClass(MainClass.class);
        job1.setMapperClass(Q7_Q8Mapper1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(MapWritable.class);
        job1.setReducerClass(Q7_Q8Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, INTERMEDIATE_OUTPUT);
        job1.waitForCompletion(true);
      
        Job job2 = Job.getInstance(conf, "job2");
        
        Path Q7_Q8_OUTPUT = new Path("/home/q7q8Output");
		
		job2.setJarByClass(MainClass.class);
        job2.setMapperClass(Q7_Q8Mapper2.class);
        job2.setMapOutputKeyClass(NullWritable.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(Q7_Q8Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2, INTERMEDIATE_OUTPUT);
        FileOutputFormat.setOutputPath(job2, Q7_Q8_OUTPUT);
        job2.waitForCompletion(true);
        
        
        Job job3 = Job.getInstance(conf, "job3");
        Path Q9_VECTORS = new Path("/home/q9Vectors");
		
		job3.setJarByClass(MainClass.class);
        job3.setMapperClass(Q9Mapper1.class);
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setReducerClass(Q9Reducer1.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job3, new Path(args[0]));
        FileOutputFormat.setOutputPath(job3, Q9_VECTORS);
        job3.waitForCompletion(true);
        
        Job job4 = Job.getInstance(conf, "job4");
        Path COS_SIM = new Path("/home/cosSim");
		
		job4.setJarByClass(MainClass.class);
        job4.setMapperClass(Q9Mapper2.class);
        job4.setMapOutputKeyClass(NullWritable.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setReducerClass(Q9Reducer2.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);
        job4.setInputFormatClass(TextInputFormat.class);
        job4.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job4, Q9_VECTORS);
        FileOutputFormat.setOutputPath(job4, COS_SIM);
        System.exit(job4.waitForCompletion(true) ? 0 : 1);
     
		
	}

}
