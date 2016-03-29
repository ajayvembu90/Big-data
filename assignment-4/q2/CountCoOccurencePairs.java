package WordCount.WordCount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

// get the co ocurrecnce of word for the twitter data set
public class CountCoOccurencePairs {
		// mapper class
	  public static class TokenizerMapper
      extends Mapper<LongWritable, Text, Text, MapWritable>{

   private MapWritable occurrenceMap = new MapWritable();
   private Text word = new Text();
   public void map(LongWritable key, Text value, Context context
                   ) throws IOException, InterruptedException {
	   int minWordLength = context.getConfiguration().getInt("minWordLength",5);
	   int neighbors = context.getConfiguration().getInt("neighbors",2);
	   String[] stopWords = context.getConfiguration().getStrings("stopWords");
	   StringTokenizer itr = new StringTokenizer(value.toString());
	   ArrayList<String> finalStringList = new  ArrayList<String>();
	   while(itr.hasMoreTokens()){
		   String currentString = itr.nextToken();
		   if (!Arrays.asList(stopWords).contains(currentString) && currentString.length() >= minWordLength)
			   finalStringList.add(currentString);
	   }
	   for (int i = 0;i<finalStringList.size();i++){
		   word.set(finalStringList.get(i));
		   occurrenceMap.clear();
		   int start = (i - neighbors < 0) ? 0 : i - neighbors;
		   int end = (i + neighbors >= finalStringList.size()) ? finalStringList.size() - 1 : i + neighbors;
		   for (int j = start; j <= end; j++) {
			   if (j == i) continue;
			   Text neighbor = new Text(finalStringList.get(j));
               if(occurrenceMap.containsKey(neighbor)){
                   IntWritable count = (IntWritable)occurrenceMap.get(neighbor);
                   count.set(count.get()+1);
                }else{
                   occurrenceMap.put(neighbor,new IntWritable(1));
                }
		   }
		   context.write(word,occurrenceMap);
	   }
   }

 }
 // reducer class
 public static class IntSumReducer
      extends Reducer<Text, MapWritable, Text, IntWritable> {
   private MapWritable incrementingMap = new MapWritable();

   protected void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
       incrementingMap.clear();
       for (MapWritable value : values) {
           addAll(value);
       }
      // context.write(key,incrementingMap);

       Set<Writable> keys = incrementingMap.keySet();
       for (Writable eachKey : keys) {
    	   IntWritable count = (IntWritable)incrementingMap.get(eachKey);
    	   context.write(new Text(key+"-"+eachKey),count);
       }

   }

   private void addAll(MapWritable mapWritable) {
       Set<Writable> keys = mapWritable.keySet();
       for (Writable key : keys) {
           IntWritable fromCount = (IntWritable) mapWritable.get(key);
           if (incrementingMap.containsKey(key)) {
               IntWritable count = (IntWritable) incrementingMap.get(key);
               count.set(count.get() + fromCount.get());
           } else {
               incrementingMap.put(key, fromCount);
           }
       }
   }
 }

 public static void main(String[] args) throws Exception {
	 // stope words to be omitted
   String[] stopWords = {"a", "about", "above", "above", "across", "after", "afterwards", "again", "against", "all", "almost", "alone", "along", "already", "also","although","always","am","among", "amongst", "amoungst", "amount",  "an", "and", "another", "any","anyhow","anyone","anything","anyway", "anywhere", "are", "around", "as",  "at", "back","be","became", "because","become","becomes", "becoming", "been", "before", "beforehand", "behind", "being", "below", "beside", "besides", "between", "beyond", "bill", "both", "bottom","but", "by", "call", "can", "cannot", "cant", "co", "con", "could", "couldnt", "cry", "de", "describe", "detail", "do", "done", "down", "due", "during", "each", "eg", "eight", "either", "eleven","else", "elsewhere", "empty", "enough", "etc", "even", "ever", "every", "everyone", "everything", "everywhere", "except", "few", "fifteen", "fify", "fill", "find", "fire", "first", "five", "for", "former", "formerly", "forty", "found", "four", "from", "front", "full", "further", "get", "give", "go", "had", "has", "hasnt", "have", "he", "hence", "her", "here", "hereafter", "hereby", "herein", "hereupon", "hers", "herself", "him", "himself", "his", "how", "however", "hundred", "ie", "if", "in", "inc", "indeed", "interest", "into", "is", "it", "its", "itself", "keep", "last", "latter", "latterly", "least", "less", "ltd", "made", "many", "may", "me", "meanwhile", "might", "mill", "mine", "more", "moreover", "most", "mostly", "move", "much", "must", "my", "myself", "name", "namely", "neither", "never", "nevertheless", "next", "nine", "no", "nobody", "none", "noone", "nor", "not", "nothing", "now", "nowhere", "of", "off", "often", "on", "once", "one", "only", "onto", "or", "other", "others", "otherwise", "our", "ours", "ourselves", "out", "over", "own","part", "per", "perhaps", "please", "put", "rather", "re", "same", "see", "seem", "seemed", "seeming", "seems", "serious", "several", "she", "should", "show", "side", "since", "sincere", "six", "sixty", "so", "some", "somehow", "someone", "something", "sometime", "sometimes", "somewhere", "still", "such", "system", "take", "ten", "than", "that", "the", "their", "them", "themselves", "then", "thence", "there", "thereafter", "thereby", "therefore", "therein", "thereupon", "these", "they", "thickv", "thin", "third", "this", "those", "though", "three", "through", "throughout", "thru", "thus", "to", "together", "too", "top", "toward", "towards", "twelve", "twenty", "two", "un", "under", "until", "up", "upon", "us", "very", "via", "was", "we", "well", "were", "what", "whatever", "when", "whence", "whenever", "where", "whereafter", "whereas", "whereby", "wherein", "whereupon", "wherever", "whether", "which", "while", "whither", "who", "whoever", "whole", "whom", "whose", "why", "will", "with", "within", "without", "would", "yet", "you", "your", "yours", "yourself", "yourselves", "the"};
   Configuration conf = new Configuration();
   //conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
   conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
   conf.set("mapreduce.framework.name", "yarn");
   conf.setInt("neighbors",5);
   conf.setInt("minWordLength",Integer.parseInt(args[2]));
   conf.setStrings("stopWords",stopWords);
   Job job = Job.getInstance(conf, "CountCoOccurencePairs");
   job.setJarByClass(CountCoOccurencePairs.class);
   job.setMapperClass(TokenizerMapper.class);
   //job.setCombinerClass(IntSumReducer.class);
   job.setMapOutputKeyClass(Text.class);
   job.setMapOutputValueClass(MapWritable.class);
   job.setReducerClass(IntSumReducer.class);
   job.setOutputKeyClass(Text.class);
   job.setOutputValueClass(IntWritable.class);
   FileInputFormat.addInputPath(job, new Path(args[0]));
   FileOutputFormat.setOutputPath(job, new Path(args[1]));
   System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}
