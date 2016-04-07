package WordCount.WordCount;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.Reducer.Context;
//import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Top10Users {
	

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: TopN <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("Top N");
        job.setJarByClass(Top10Users.class);
        job.setMapperClass(TopNMapper.class);
        //job.setCombinerClass(TopNReducer.class);
        job.setReducerClass(TopNReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TopNMapper extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	FileSplit fileSplit = (FileSplit) context.getInputSplit();
        	String filename = fileSplit.getPath().getName();
        	String[] fields = value.toString().split(("\\^"));
        	if (filename.equals("user.csv"))
        			context.write(new Text(fields[0]), new Text(fields[1]));
        	else
        		context.write(new Text(fields[1]), new Text(one.toString()));
        }
    }

    public static class TopNReducer extends Reducer<Text, Text, Text, Text> {
    	
    	private Map<Text, IntWritable> countMap = new HashMap<Text, IntWritable> ();
    	private Map<Text, Text> userNameMap = new HashMap<Text, Text> ();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        	int totalCount = 0;
        	String userName = "";
            for (Text val : values){
            	int tempTotalCount = totalCount;
            	try{
            		totalCount += Integer.parseInt(val.toString());
            	}catch(NumberFormatException nfe){
            		userName = val.toString();
            		totalCount = tempTotalCount; 
            		continue;
            	}
            	
            }
            userNameMap.put(new Text(key), new Text(userName));
            countMap.put(new Text(key), new IntWritable(totalCount));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            Map<Text, IntWritable> sortedMap = sortByValues(countMap);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 10) {
                    break;
                }
                context.write(key, userNameMap.get(key));
            }
        }
    }

    private static <K extends Comparable<? super K>, V extends Comparable<? super V>> Map<K, V> sortByValues(Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

            
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });
        
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();

        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
}
