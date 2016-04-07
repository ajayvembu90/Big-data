package WordCount.WordCount;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Q2A5 {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("userName", args[0]);
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 4) {
            System.err.println("Usage: TopN <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("average user rating");
        job.setJarByClass(Q2A5.class);
        job.setMapperClass(UserAverageRatingMapper.class);
        //job.setCombinerClass(TopNReducer.class);
        job.setReducerClass(UserAverageRatingReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[2]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    public static class  UserAverageRatingMapper extends Mapper<Object, Text, Text, Text> {
        
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	Configuration conf = context.getConfiguration();
        	FileSplit fileSplit = (FileSplit) context.getInputSplit();
        	String filename = fileSplit.getPath().getName();
        	String userName = conf.get("userName");
        	String[] fields = value.toString().split(("\\^"));
        	
        	if (filename.equals("user.csv")){
            	if (fields[1].equals(userName))
            		context.write(new Text(fields[0]), new Text(fields[1]));
        	}
        	else
        		context.write(new Text(fields[1]), new Text(fields[3]));
        }
        
    }
    
    public static class UserAverageRatingReducer extends Reducer<Text, Text, Text, FloatWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // computes the number of occurrences of a single word
            float sum = 0;
            int count = 0; 
            String userName = null;
            for (Text val : values) {
            	float tempSum = sum;
            	try{
            		sum += Float.parseFloat(val.toString());;
            		count++;
            	}catch(NumberFormatException nfe){
            		userName = val.toString();
            		sum = tempSum;
            		continue;
            	}
            }
            float average;
            if (count == 0)
            	average = 0;
            else
            	average = sum / (float)count;
            // puts the number of occurrences of this word into the map.
            // We need to create another Text object because the Text instance
            // we receive is the same for all the words
            if (userName != null)
            	context.write(new Text(userName), new FloatWritable(average));
        }
    }

    
}
