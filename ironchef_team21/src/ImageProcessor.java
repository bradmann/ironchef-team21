import java.io.ByteArrayInputStream;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.marklogic.mapreduce.ContentOutputFormat;
import com.marklogic.mapreduce.DocumentInputFormat;
import com.marklogic.mapreduce.DocumentURI;
import com.marklogic.mapreduce.KeyValueInputFormat;
import com.marklogic.mapreduce.MarkLogicInputFormat;
import com.marklogic.mapreduce.MarkLogicNode;
import com.marklogic.mapreduce.NodePath;
import com.marklogic.mapreduce.ValueInputFormat;

public class ImageProcessor {

	public static class PHashMapper extends Mapper<Text, Text, DocumentURI, Text>{
	    
		public void map(Text key, Text value, Context context) {
			try {
				ImagePHash phash = new ImagePHash();
				InputStream is = new ByteArrayInputStream(value.toString().getBytes());		
				Text hash = new Text(phash.getHash(is));
				String newkey = key.toString() + "_phash";
				Logger.getRootLogger().info(value.toString().length());
				Logger.getRootLogger().info(newkey);
				Logger.getRootLogger().info(hash);
	           	context.write(new DocumentURI(newkey), hash);
            } catch (Exception e) {
            	e.printStackTrace();
            }
		}
	}
	
	public static void main(String[] args) throws Exception {
		//This is the line that makes the hadoop run locally
		//conf.set("mapred.job.tracker", "local");
		
		Configuration conf = new Configuration();

        Job job = new Job(conf, "Process Image Hash");
        job.setJarByClass(ImageProcessor.class);
        job.setInputFormatClass(KeyValueInputFormat.class);
        job.setMapperClass(PHashMapper.class);
        job.setMapOutputKeyClass(DocumentURI.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputFormatClass(ContentOutputFormat.class);
        conf.set("mapred.reduce.tasks", "0");
        
        
        conf = job.getConfiguration();
        conf.addResource("ImageProcessor.xml");
    
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}