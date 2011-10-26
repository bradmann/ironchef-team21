import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import javax.imageio.ImageIO;

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
import org.apache.hadoop.mapreduce.Reducer;

import com.googlecode.javacpp.Loader;
import com.googlecode.javacv.cpp.opencv_core.CvMemStorage;
import com.googlecode.javacv.cpp.opencv_core.CvSeq;
import com.googlecode.javacv.cpp.opencv_core.IplImage;
import com.googlecode.javacv.cpp.opencv_objdetect;
import com.googlecode.javacv.cpp.opencv_objdetect.CvHaarClassifierCascade;


import static com.googlecode.javacv.cpp.opencv_core.*;
import static com.googlecode.javacv.cpp.opencv_imgproc.*;
import static com.googlecode.javacv.cpp.opencv_objdetect.*;
import static com.googlecode.javacv.cpp.opencv_highgui.*;
public class FaceDetector {

	public static class FaceDetectMapper extends MapReduceBase implements Mapper<Text, BytesWritable, Text, CvSeq>{
		public static final int SUBSAMPLING_FACTOR = 4;
		private IplImage inputImage;
	    private CvHaarClassifierCascade classifier;
	    private CvMemStorage storage;
	    private CvSeq faces;
	    
		public void map(Text key, BytesWritable value, OutputCollector<Text, CvSeq> output, Reporter reporter) {
			CvSeq faceSequence;
			try {
				//Detect faces in the image
				faceSequence = detectFaces(value.getBytes());
				
				//Put the file in the map where the key is the original key and the value is the
				//sequence of faces found
				output.collect(key, faceSequence);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		public CvSeq detectFaces(byte[] imageData) throws IOException {
			File classifierFile = new File("res/haarcascade_frontalface_alt.xml", "r");
			// Preload the opencv_objdetect module to work around a known bug.
	        Loader.load(opencv_objdetect.class);
	        classifier = new CvHaarClassifierCascade(cvLoad(classifierFile.getAbsolutePath()));
	        classifierFile.delete();
	        if (classifier.isNull()) {
	            throw new IOException("Could not load the classifier file.");
	        }
	        storage = CvMemStorage.create();
	        BufferedImage image = ImageIO.read ( new ByteArrayInputStream ( imageData ) );
	        
	        inputImage = IplImage.createFrom(image);

	        faces = cvHaarDetectObjects(inputImage, classifier, storage, 1.1, 3, CV_HAAR_DO_CANNY_PRUNING);
	        cvClearMemStorage(storage);
	        
			return faces;
		}


		//Old sample code for calculating the md5hash of an image
		static String calculateMd5(byte[] imageData) throws NoSuchAlgorithmException {
			//get the md5 for this specific data
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(imageData);
			byte[] hash = md.digest();
			// Below code of converting Byte Array to hex
			String hexString = new String();
			for (int i=0; i < hash.length; i++) {
				hexString += Integer.toString( ( hash[i] & 0xff ) + 0x100, 16).substring( 1 );
			}
			return hexString;
		}

	}
	
	public static class ImageDupsReducer extends Reducer<Text,Text,Text,Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//Key here is the md5 hash while the values are all the image files that
			// are associated with it. for each md5 value we need to take only
			// one file (the first)
			Text imageFilePath = null;
			for (Text filePath : values) {
				imageFilePath = filePath;
				break;//only the first one
			}
			// In the result file the key will be again the image file path. 
			context.write(imageFilePath, key);
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
		JobConf job = new JobConf(FaceDetector.class);
		job.setJobName("DetectFaces");
		job.setInputFormat(SequenceFileInputFormat.class);
		job.setMapperClass(FaceDetectMapper.class);
		//job.setNumReduceTasks(2);
		//job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.wait();
	}
}