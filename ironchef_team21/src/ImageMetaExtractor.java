import java.io.StringWriter;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.metadata.IIOMetadata;
import javax.imageio.stream.ImageInputStream;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.w3c.dom.Node;

public class ImageMetaExtractor {

	public static class ImageMetaMapper extends MapReduceBase implements Mapper<Text, BytesWritable, Text, Text>{
	    
		public void map(Text key, BytesWritable value, OutputCollector<Text, Text> output, Reporter reporter) {
			try {	
				ImageInputStream iis = ImageIO.createImageInputStream(value);
            	Iterator<ImageReader> readers = ImageIO.getImageReaders(iis);
	            if (readers.hasNext()) {
	
	                // pick the first available ImageReader
	                ImageReader reader = readers.next();
	
	                // attach source to the reader
	                reader.setInput(iis, true);
	
	                // read metadata of first image
	                IIOMetadata metadata = reader.getImageMetadata(0);
	
	                String[] names = metadata.getMetadataFormatNames();
	                StringBuilder xmlStringBuilder = new StringBuilder();
	                int length = names.length;
	                for (int i = 0; i < length; i++) {
	                	Node node = metadata.getAsTree(names[i]);
	                	Transformer t = TransformerFactory.newInstance().newTransformer();
	                    t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
	                    StringWriter sw = new StringWriter();
	                    t.transform(new DOMSource(node), new StreamResult(sw));
	                    xmlStringBuilder.append(sw.toString());
	                }
	                Text xmlOutput = new Text(xmlStringBuilder.toString());
	                output.collect(key, xmlOutput);
	            }
            } catch (Exception e) {
            	e.printStackTrace();
            }
		}
	}
	
	public static void main(String[] args) throws Exception {

		//This is the line that makes the hadoop run locally
		//conf.set("mapred.job.tracker", "local");
		/*String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}*/
		JobConf job = new JobConf(ImageMetaExtractor.class);
		job.setJobName("DetectFaces");
		job.setInputFormat(SequenceFileInputFormat.class);
		job.setMapperClass(ImageMetaMapper.class);
		//job.setNumReduceTasks(2);
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.wait();
	}
}