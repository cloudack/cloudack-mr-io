/**
 * 
 */
package com.cloudack.io;

/**
 * @author pudi
 *
 */
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jdom2.Document;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

/**
 * 
 * @author pudi
 */
public class WordCountXmlMapper extends
		Mapper<LongWritable, Text, NullWritable, Text> {

	@Override
	public void map(LongWritable key, Text value1, Context context)

	throws IOException, InterruptedException {

		String xmlString = value1.toString();

		SAXBuilder builder = new SAXBuilder();
		Reader in = new StringReader(xmlString);
		String value = "";
		try {

			Document doc = builder.build(in);
			Element root = doc.getRootElement();
			
			//TODO add your own DOM parsing logic

			context.write(NullWritable.get(), new Text(value));
		} catch (JDOMException ex) {
			Logger.getLogger(WordCountXmlMapper.class.getName()).log(
					Level.SEVERE, null, ex);
		} catch (IOException ex) {
			Logger.getLogger(WordCountXmlMapper.class.getName()).log(
					Level.SEVERE, null, ex);
		}

	}

}
