/**
 * 
 */
package com.cloudack.io.ff;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileAsTextInputFormat;
import org.apache.log4j.Logger;


/**
 * @author pudi
 *
 */
public class EngDictInputSeq {
	
	public static class WordCountMapper extends
			Mapper<Text, Text, Text, IntWritable> {
		private static final Logger sLogger = Logger
				.getLogger(WordCountMapper.class);
		private final static IntWritable one = new IntWritable(1);
		

		public void map(Text key, Text value, Context context)
				throws IOException, InterruptedException {
			
			context.write(key, one);
			
		}
	}
	
	public static class WordCountReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "wordcountseqinput");
		job.setJarByClass(EngDictInputSeq.class);
		
		job.setMapperClass(WordCountMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setReducerClass(WordCountReducer.class);
		
		job.setInputFormatClass(SequenceFileAsTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path("data/output/dictseq/part*"));
		FileOutputFormat.setOutputPath(job, new Path("data/output/text"));

		job.waitForCompletion(true);
	}

}
