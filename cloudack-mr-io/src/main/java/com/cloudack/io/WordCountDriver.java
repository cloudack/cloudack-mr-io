/**
 * 
 */
package com.cloudack.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/**
 * @author pudi
 * 
 */
public class WordCountDriver {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.compress.map.output", "true");
		conf.set("mapred.output.compression.codec","org.apache.hadoop.io.compress.BZip2Codec");
		
		Job job = new Job(conf, "wordcount with output compression");

		job.setJarByClass(WordCountDriver.class);
		
	
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		
		
	/*	
		FileOutputFormat.setCompressOutput(job, true);
	    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
	    
	    FileOutputFormat.setOutputCompressorClass(job, BZip2Codec.class);
	   */

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
