package WordCount.WordCount;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class BusinessInTexas {
	

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: TopN <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("Top N");
        job.setJarByClass(BusinessInTexas.class);
        job.setMapperClass(BusinessInTexasMapper.class);
        //job.setCombinerClass(TopNReducer.class);
        job.setReducerClass(BusinessInTexasReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    public static class BusinessInTexasMapper extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	FileSplit fileSplit = (FileSplit) context.getInputSplit();
        	String filename = fileSplit.getPath().getName();
        	String[] fields = value.toString().split(("\\^"));
        	if (filename.equals("business.csv")){
        		if (fields[1].toLowerCase().contains("texas") || fields[1].toLowerCase().contains("tx") )
        			context.write(new Text(fields[0]), new Text("present"));
        	}
        	else
        		context.write(new Text(fields[2]), new Text(one.toString()));
        }
    }

    public static class BusinessInTexasReducer extends Reducer<Text, Text, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // computes the number of occurrences of a single word
        	int totalCount = 0;
        	boolean presentInd = false;
            for (Text val : values){
            	if (val.toString().equals("present"))
            		presentInd = true;
            	else
            		totalCount++;
            }
            if (presentInd)
            	context.write(new Text(key), new IntWritable(totalCount));
        }

    }

}
