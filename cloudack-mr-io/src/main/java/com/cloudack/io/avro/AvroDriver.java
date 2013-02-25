/**
 * 
 */
package com.cloudack.io.avro;

/**
 * @author pudi
 *
 */


import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.mapred.AvroJob;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class AvroDriver extends Configured implements Tool {

  public int run(String[] args) throws Exception {
    
    if (args.length != 3) {
      System.err.printf("Usage: %s [generic options] <input> <output> <schema-file>\n",
          getClass().getSimpleName());
      ToolRunner.printGenericCommandUsage(System.err);
      return -1;
    }
    
    String input = args[0];
    String output = args[1];
    String schemaFile = args[0];

    JobConf conf = new JobConf(getConf(), getClass());
    conf.setJobName("Avro Map Reduce");
    
    FileInputFormat.addInputPath(conf, new Path(input));
    FileOutputFormat.setOutputPath(conf, new Path(output));
    
    GenericDatumReader datum = new GenericDatumReader();
    DataFileReader reader = new DataFileReader(new File(schemaFile), datum);
    
    Schema schema = reader.getSchema();
    
    
    AvroJob.setInputSchema(conf, schema);
    AvroJob.setMapOutputSchema(conf, schema);
    AvroJob.setOutputSchema(conf, schema);
    conf.setNumReduceTasks(0);

    JobClient.runJob(conf); 
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new AvroDriver(), args);
    System.exit(exitCode);
  }

}