package JavaHDFS.JavaHDFS;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.util.Progressable;

public class FileDownloadUploadDecompress {

	public static void main(String[] args)throws Exception{
		String[] books = {"20417.txt.bz2","5000-8.txt.bz2","132.txt.bz2","1661-8.txt.bz2","972.txt.bz2","19699.txt.bz2"};
		FileOutputStream fos = null;

		// code to downlaod from the URL to the local
		for (String eachBook : books ){
			URL website = new URL("http://www.utdallas.edu/~axn112530/cs6350/lab2/input/"+eachBook);
			ReadableByteChannel rbc = Channels.newChannel(website.openStream());
			fos = new FileOutputStream(eachBook);
			fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
		}
		fos.close();

    // code to download and upload to the HDFS
		for (String eachBook : books ){
			InputStream in = new BufferedInputStream(new FileInputStream(eachBook));
			Configuration conf = new Configuration();
			String dst = "/user/axv143730/"+eachBook;
		    conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
		    conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
		    conf.set("fs.hdfs.impl",
		            org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
		        );
		    conf.set("fs.file.impl",
		            org.apache.hadoop.fs.LocalFileSystem.class.getName()
		        );
		    FileSystem fs = FileSystem.get(URI.create(dst), conf);
		    OutputStream out = fs.create(new Path(dst), new Progressable() {
		        public void progress() {
		          System.out.print(".");
		        }
		    });
		    IOUtils.copyBytes(in, out,conf);
		}
		// code to decompress the file in HDFS
		for (String eachBook : books ){
			String uri = "/user/axv143730/"+eachBook;
			Configuration conf = new Configuration();
			conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/core-site.xml"));
		    conf.addResource(new Path("/usr/local/hadoop-2.4.1/etc/hadoop/hdfs-site.xml"));
		    conf.set("fs.hdfs.impl",
		            org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
		        );
		    conf.set("fs.file.impl",
		            org.apache.hadoop.fs.LocalFileSystem.class.getName()
		        );
			FileSystem fs = FileSystem.get(URI.create(uri), conf);
			Path inputPath = new Path(uri);
			CompressionCodecFactory factory = new CompressionCodecFactory(conf);
		    CompressionCodec codec = factory.getCodec(inputPath);
		    if (codec == null) {
			      System.err.println("No codec found for " + uri);
			      System.exit(1);
			}
		    String outputUri =
		  	      CompressionCodecFactory.removeSuffix(uri, codec.getDefaultExtension());
		    InputStream in = null;
		    OutputStream out = null;
		    try {
			      in = codec.createInputStream(fs.open(inputPath));
			      out = fs.create(new Path(outputUri));
			      IOUtils.copyBytes(in, out, conf);
			}finally {
			      IOUtils.closeStream(in);
			      IOUtils.closeStream(out);
			}
		}
	}
}
