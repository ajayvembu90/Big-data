import java.io.IOException;
import java.io.PrintStream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
// to get the business that are in NY 
public class CountYelpBusiness
{
  // mapper class
  public static class BusinessMap
    extends Mapper<Object, Text, Text, Text>
  {
    public void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
      throws IOException, InterruptedException
    {
      String[] data = value.toString().split("\\^");
      if (data.length >= 3)
      {
        String businessid = data[0];
        String address = data[1].toLowerCase();
        if (address.contains("ny")) {
          context.write(new Text(businessid), new Text(address));
        }
      }
    }
  }

  public static void main(String[] args)
    throws Exception
  {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2)
    {
      System.err.println("Usage: BusinessId <input path> <output path>");
      System.exit(2);
    }
    Path input = new Path(args[0]);
    Path output = new Path(args[1]);
    FileSystem fs = FileSystem.get(conf);
    if (fs.isDirectory(output)) {
      fs.delete(output, true);
    }
    Job job = Job.getInstance(conf, "BusinessId");
    job.setJarByClass(CountYelpBusiness.class);
    job.setMapperClass(CountYelpBusiness.BusinessMap.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, input);
    FileOutputFormat.setOutputPath(job, output);
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
