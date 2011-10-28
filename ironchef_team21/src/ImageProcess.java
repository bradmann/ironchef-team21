import java.io.File;
import java.io.FileInputStream;

import javax.imageio.stream.FileImageInputStream;
import javax.imageio.stream.ImageInputStream;


public class ImageProcess {
	public static void main(String[] args) {
		try {
			ImagePHash phash = new ImagePHash();
			File f = new File("/Users/bradmann/Downloads/Basketball.png");
			FileInputStream iis = new FileInputStream(f);
		
			String hash = phash.getHash(iis);
			System.out.println(hash);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
