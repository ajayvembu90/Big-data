package WordCount.WordCount;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Progressable;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;

// code the read the past seven days of twitter data and count the words
public class ReadTwitterDataAndCountWord {
	 // mapper code to tokenize each word in the twitter feed file
   public static class TokenizerMapper
      extends Mapper<Object, Text, Text, IntWritable>{

	 // private state objects which are maintained across all the mappers
   private final static IntWritable one = new IntWritable(1);
   private Text word = new Text();

   public void map(Object key, Text value, Context context
                   ) throws IOException, InterruptedException {
     StringTokenizer itr = new StringTokenizer(value.toString());
     while (itr.hasMoreTokens()) {
       String currentString = itr.nextToken();
       if (currentString.charAt(0) == '#'){
    	   word.set(currentString);
           context.write(word, one);
       }
     }
    }
   }

	 // reducer code to sum up the words
   public static class IntSumReducer
        extends Reducer<Text,IntWritable,Text,IntWritable> {
		 // result state to maintain the sum
     private IntWritable result = new IntWritable();

     public void reduce(Text key, Iterable<IntWritable> values,
                        Context context
                        ) throws IOException, InterruptedException {
       int sum = 0;
       for (IntWritable val : values) {
         sum += val.get();
       }

       result.set(sum);
       context.write(key, result);
     }
   }

	public static void main(String[] args)throws TwitterException,IOException,InterruptedException,ClassNotFoundException{
		Twitter twitter = new TwitterFactory().getInstance();

	    AccessToken accessToken = new AccessToken("2840901034-fZ2Zjm2KrlOfAoQ2lRprAZ6VXBjtpcbejaP5rBw", "8okZQoi3dFcMT6i8TD3Guts9JRh8r3eM6529lNPgQQOsf");
	    twitter.setOAuthConsumer("UoYPpaFSXziUntLZ6tnJJbudp", "SUoB2KextuUITXwEWlnanZ1VunFxa1IaFqYA52xO7OWTv2tnHH");
	    twitter.setOAuthAccessToken(accessToken);
	    String[] sinceArray = {"2016-01-29","2016-01-31","2016-02-02","2016-02-04","2016-02-06","2016-01-27"};
	    String[] untilArray = {"2016-01-30","2016-02-01","2016-02-03","2016-02-05","2016-02-07","2016-01-28"};
	    Query query = new Query("fun");
	    FileWriter fw = null;

	    // to download the twitter files
	    for(int j=0;j<6;j++){
		    fw = new FileWriter(new File("movie_tweets_from_"+sinceArray[j]+"_to_"+untilArray[j]+".txt"));
	      //  fw = new FileWriter(new File("test"));
	        int i=1;
		    while(i<=20 && query!=null){
			      query.setCount(100);
			      query.setSince(sinceArray[j]);
			      query.setUntil(untilArray[j]);
			      QueryResult result;
			      result = twitter.search(query);
			      List<Status> tweets = result.getTweets();
			      for (Status tweet : tweets) {
			            String currentTweetString = "@" + tweet.getUser().getScreenName() + " - " + tweet.getText();
			            fw.write(currentTweetString);
			            fw.write("\n");
			      }
			      query= result.nextQuery();
			      i++;
		    }
	    }
	    fw.close();

	    // to upload all the files into the hdfs server
	    for(int j=0;j<6;j++){
	    	String tweetFile = "movie_tweets_from_"+sinceArray[j]+"_to_"+untilArray[j]+".txt";
	    	InputStream in = new BufferedInputStream(new FileInputStream(tweetFile));
	    	Configuration conf = new Configuration();
	    	String dst = "/user/axv143730/assignment2/tweetsinputfolder/"+tweetFile;
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

	    // to get the word count

	    Configuration conf = new Configuration();
	    //conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
	    conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
	    conf.set("mapreduce.framework.name", "yarn");
	    Job job = Job.getInstance(conf, "word count");
	    job.setJarByClass(WordCount.class);
	    job.setMapperClass(TokenizerMapper.class);
	    job.setCombinerClass(IntSumReducer.class);
	    job.setReducerClass(IntSumReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[1]));
	    FileOutputFormat.setOutputPath(job, new Path(args[2]));
	    System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
