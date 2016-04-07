package WordCount.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;


public class TopNBusiness {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 3) {
            System.err.println("Usage: TopN <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("Top N");
        job.setJarByClass(TopNBusiness.class);
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

    /**
     * The mapper reads one line at the time, splits it into an array of single words and emits every
     * word to the reducers with the value of 1.
     */
    public static class TopNMapper extends Mapper<Object, Text, Text, Text> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String tokens = "[_|$#<>\\^=\\[\\]\\*/\\\\,;,.\\-:()?!\"']";

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	FileSplit fileSplit = (FileSplit) context.getInputSplit();
        	String filename = fileSplit.getPath().getName();
        	String[] fields = value.toString().split(("\\^"));
        	if (filename.equals("business.csv"))
        		context.write(new Text(fields[0]), new Text(fields[1] + "^"+fields[2]));
        	else
        		context.write(new Text(fields[2]), new Text(fields[3]));
        }
    }

    /**
     * The reducer retrieves every word and puts it into a Map: if the word already exists in the
     * map, increments its value, otherwise sets it to 1.
     */
    public static class TopNReducer extends Reducer<Text, Text, Text, Text> {

        private Map<Text, Text> countMap = new HashMap<Text, Text> ();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            // computes the number of occurrences of a single word
            float sum = 0;
            int count = 0; 
            String[] result=null;
            for (Text val : values) {
            	float tempSum = sum;
            	try{
            		sum += Float.parseFloat(val.toString());
            		count++;
            	}catch(NumberFormatException nfe){
            		result = val.toString().split("\\^");
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
            countMap.put(new Text(key), new Text("^"+result[0]+"^"+result[1]+"^"+average));
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {

            Map<Text, Text> sortedMap = sortByValues(countMap);

            int counter = 0;
            for (Text key : sortedMap.keySet()) {
                if (counter++ == 20) {
                    break;
                }
                context.write(key, sortedMap.get(key));
            }
        }
    }

    /*
   * sorts the map by values. Taken from:
   * http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html
   */
    private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues1(Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

            
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();

        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
    
    /*
   * sorts the map by values. Taken from:
   * http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html
   */
    private static <K extends Comparable> Map<K, Text> sortByValues(Map<K, Text> map) {
        List<Map.Entry<K, Text>> entries = new LinkedList<Map.Entry<K, Text>>(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, Text>>() {

            
            public int compare(Map.Entry<K, Text> o1, Map.Entry<K, Text> o2) {
            	String[] s1 = o1.getValue().toString().split("\\^");
            	String[] s2 = o2.getValue().toString().split("\\^");
            	Float v1 = Float.parseFloat(s1[3]),v2 = Float.parseFloat(s2[3]);
                return v2.compareTo(v1);
            	//return v1 - v2;
            }
        });

        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, Text> sortedMap = new LinkedHashMap<K, Text>();

        for (Map.Entry<K, Text> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }

}