/**
 * 
 */
package com.cloudack.io.avro;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroCollector;
import org.apache.avro.mapred.AvroMapper;
import org.apache.avro.mapred.Pair;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapred.Reporter;

/**
 * @author pudi
 *
 */
public class AvroCustomMapper extends AvroMapper<Utf8, Pair<Integer, GenericRecord>> {
	
	public void map(Utf8 line,
        AvroCollector<Pair<DoubleWritable, GenericRecord>> collector,
        Reporter reporter) throws IOException {
		
     
    }

}
