import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// uses the standard business data set
// counts the business that are present in the PALO
public class CountYelpBusiness
{
  // mapper class
  public static class BusinessMap
    extends Mapper<LongWritable, Text, Text, IntWritable>
  {
    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, IntWritable>.Context context)
      throws IOException, InterruptedException
    {
      String[] businessData = value.toString().split("\\^");
      String businessid = businessData[1];
      String address = businessData[1].toLowerCase();
      if (address.contains("palo")) {
        // write the business id's which are in the palo to the context
        context.write(new Text(businessid), new IntWritable(1));
      }
    }

    protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
      throws IOException, InterruptedException
    {}
  }
  // reduce class
  public static class Reduce
    extends Reducer<Text, IntWritable, Text, IntWritable>
  {
    // sum up the business which have the same businessid which are in PALO
    public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context)
      throws IOException, InterruptedException
    {
      int count = 0;
      for (IntWritable t : values) {
        count++;
      }
      context.write(key, new IntWritable(count));
    }
  }

  public static void main(String[] args)
    throws Exception
  {
    // configure the job
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2)
    {
      System.err.println("Usage: BusinessMap <in> <out>");
      System.exit(2);
    }
    Path input = new Path(args[0]);
    Path output = new Path(args[1]);
    FileSystem fs = FileSystem.get(conf);
    if (fs.isDirectory(output)) {
      fs.delete(output, true);
    }
    Job job = Job.getInstance(conf, "BusinessMap");
    job.setJarByClass(CountYelpBusiness.class);

    job.setMapperClass(CountYelpBusiness.BusinessMap.class);
    job.setReducerClass(CountYelpBusiness.Reduce.class);

    job.setOutputKeyClass(Text.class);

    job.setMapOutputValueClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, input);

    FileOutputFormat.setOutputPath(job, output);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
