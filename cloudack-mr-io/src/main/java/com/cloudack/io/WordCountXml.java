/**
 * 
 */
package com.cloudack.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.streaming.StreamInputFormat;
import org.apache.mahout.classifier.bayes.XmlInputFormat;

/**
 * @author pudi
 *
 */
public class WordCountXml {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws ClassNotFoundException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		//TODO read XML files in this map reduce job
		Configuration conf = new Configuration();
	     
	       	conf.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader");
			conf.set("stream.recordreader.begin", "<row>");
			conf.set("stream.recordreader.end", "</row>");

			
			Job job = new Job(conf, "wordcount with multiline input");
	
			job.setJarByClass(WordCountDriver.class);
			
		
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);
			
			
			job.setMapperClass(WordCountMapper.class);
			job.setReducerClass(WordCountReducer.class);
			
			job.setInputFormatClass(XmlInputFormat.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			
			
			
			/*[*/FileOutputFormat.setCompressOutput(job, true);
		    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);/*]*/
		    
	
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
	
			job.waitForCompletion(true);
			
		
		
		

	}

}
