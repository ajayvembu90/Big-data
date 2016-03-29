import java.io.IOException;
import java.io.PrintStream;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
// to get the top 10 zip codes that does business
public class TopN
{
  public static void main(String[] args)
    throws Exception
  {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2)
    {
      System.err.println("Usage: TopN <in> <out>");
      System.exit(2);
    }
    Job job = Job.getInstance(conf);
    job.setJobName("Top N");
    job.setJarByClass(TopN.class);
    job.setMapperClass(TopN.TopNMapper.class);

    job.setReducerClass(TopN.TopNReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

  // mapper class
  public static class TopNMapper
    extends Mapper<Object, Text, Text, IntWritable>
  {
    public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
      throws IOException, InterruptedException
    {
      String[] data = value.toString().split("\\^");
      if (data.length >= 3)
      {
        // get the ZIP codes
        String zipcode = data[1].toLowerCase().substring(data[1].length() - 5, data[1].length());

        context.write(new Text(zipcode), new IntWritable(1));
      }
    }
  }
  // reducer class
  public static class TopNReducer
    extends Reducer<Text, IntWritable, Text, IntWritable>
  {
    private Map<Text, IntWritable> countMap = new HashMap();

    public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
      throws IOException, InterruptedException
    {
      // sum up the ZIP codes by the key value
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      this.countMap.put(new Text(key), new IntWritable(sum));
    }

    // clean up method to sort all the zip codes by value
    protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context)
      throws IOException, InterruptedException
    {
      Map<Text, IntWritable> sortedMap = TopN.sortByValues(this.countMap);

      int counter = 0;
      for (Text key : sortedMap.keySet())
      {
        if (counter++ == 10) {
          break;
        }
        context.write(key, (IntWritable)sortedMap.get(key));
      }
    }
  }
  // code for sorting
  public static class TopNCombiner
    extends Reducer<Text, IntWritable, Text, IntWritable>
  {
    public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
      throws IOException, InterruptedException
    {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      context.write(key, new IntWritable(sum));
    }
  }

  private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map)
  {
    List<Map.Entry<K, V>> entries = new LinkedList(map.entrySet());

    Collections.sort(entries, new Comparator()
    {
      public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2)
      {
        return ((Comparable)o2.getValue()).compareTo(o1.getValue());
      }
    });
    Map<K, V> sortedMap = new LinkedHashMap();
    for (Map.Entry<K, V> entry : entries) {
      sortedMap.put((Comparable)entry.getKey(), (Comparable)entry.getValue());
    }
    return sortedMap;
  }
}
