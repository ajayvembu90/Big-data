package WordCount.WordCount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q3A5 {

	 public static void main(String[] args) throws Exception {
	        Configuration conf = new Configuration();
	        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
	        if (otherArgs.length != 3) {
	            System.err.println("Usage: TopN <in> <out>");
	            System.exit(2);
	        }
	        Job job = Job.getInstance(conf);
	        job.setJobName("average user rating");
	        job.setJarByClass(Q3A5.class);
	        job.setMapperClass(FilterByBusinessMapper.class);
	        //job.setCombinerClass(TopNReducer.class);
	        job.setReducerClass(FilterByBusinessReducer.class);
	        job.setOutputKeyClass(Text.class);
	        job.setOutputValueClass(Text.class);
	        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
	        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
	        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
	        System.exit(job.waitForCompletion(true) ? 0 : 1);
	    }
	    
	    public static class  FilterByBusinessMapper extends Mapper<Object, Text, Text, Text> {
	        
	        @Override
	        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	        	FileSplit fileSplit = (FileSplit) context.getInputSplit();
	        	String filename = fileSplit.getPath().getName();
	        	String[] fields = value.toString().split(("\\^"));
	        	
	        	if (filename.equals("business.csv")){
	            	if (fields[1].toLowerCase().contains("stanford"))
	            		context.write(new Text(fields[0]), new Text("present"));
	        	}
	        	else
	        		context.write(new Text(fields[2]), new Text(fields[1]+"^"+fields[3]));
	        }
	        
	    }
	    
	    public static class FilterByBusinessReducer extends Reducer<Text, Text, Text, Text> {

	        @Override
	        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

	        	List<String> s = new ArrayList<String>();
	        	boolean presentInd = false;
	            for (Text val : values) {
	            	if (val.toString().equals("present"))
	            		presentInd = true;
	            	else
	            		s.add(val.toString());
	            }
	            if (presentInd){
	            	for (String eachString : s){
	        			String[] result = eachString.split("\\^");
	        			context.write(new Text(result[0]), new Text(result[1]));
	            	}	
	            }
	            	
	        }
	    }
}
