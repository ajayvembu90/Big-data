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
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
// get the top 10 reviewed businessId for the YELP dataset

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
    job.setOutputValueClass(FloatWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  // mapper class
  public static class TopNMapper
    extends Mapper<Object, Text, Text, FloatWritable>
  {
    public void map(Object key, Text value, Mapper<Object, Text, Text, FloatWritable>.Context context)
      throws IOException, InterruptedException
    {
      String[] data = value.toString().split("\\^");
      if (data.length >= 3)
      {
        String businessId = data[2];
        context.write(new Text(businessId), new FloatWritable(Float.parseFloat(data[3])));
      }
    }
  }
  // reducer class
  public static class TopNReducer
    extends Reducer<Text, FloatWritable, Text, FloatWritable>
  {
    private Map<Text, FloatWritable> countMap = new HashMap();

    public void reduce(Text key, Iterable<FloatWritable> values, Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
      throws IOException, InterruptedException
    {
      float sum = 0.0F;
      int counter = 0;
      for (FloatWritable val : values)
      {
        // get the count of it
        sum += val.get();
        counter++;
      }
      this.countMap.put(new Text(key), new FloatWritable(sum / counter));
    }
    // sorting happens here after the reduce step
    protected void cleanup(Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
      throws IOException, InterruptedException
    {
      Map<Text, FloatWritable> sortedMap = TopN.sortByValues(this.countMap);

      int counter = 0;
      for (Text key : sortedMap.keySet())
      {
        if (counter++ == 10) {
          break;
        }
        context.write(key, (FloatWritable)sortedMap.get(key));
      }
    }
  }

  public static class TopNCombiner
    extends Reducer<Text, FloatWritable, Text, FloatWritable>
  {
    public void reduce(Text key, Iterable<FloatWritable> values, Reducer<Text, FloatWritable, Text, FloatWritable>.Context context)
      throws IOException, InterruptedException
    {
      int sum = 0;
      for (FloatWritable val : values) {
        sum = (int)(sum + val.get());
      }
      context.write(key, new FloatWritable(sum));
    }
  }

  private static <K extends Comparable, V extends Comparable> Map<K, V> sortByValues(Map<K, V> map)
  {
    List<Map.Entry<K, V>> entries = new LinkedList(map.entrySet());

    Collections.sort(entries, new Comparator()
    {
      public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2)
      {
        return ((Comparable)o1.getValue()).compareTo(o2.getValue());
      }
    });
    Map<K, V> sortedMap = new LinkedHashMap();
    for (Map.Entry<K, V> entry : entries) {
      sortedMap.put((Comparable)entry.getKey(), (Comparable)entry.getValue());
    }
    return sortedMap;
  }
}
