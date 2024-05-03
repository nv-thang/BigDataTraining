package kiemtra;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.LongWritable;

public class Frequency {
	public static class FrequencyMapper
	  extends Mapper<LongWritable, Text, Text, IntWritable> {

	  private final static IntWritable one = new IntWritable(1);

	  @Override
	  public void map(LongWritable offset, Text lineText, Context context)
	      throws IOException, InterruptedException {

	    String line = lineText.toString();
	    String eventID = line.split(",")[1];
	    context.write(new Text(eventID), one);
	  }
	}
	
	public static class FrequencyReducer extends Reducer<Text ,  IntWritable ,  Text ,  IntWritable > {
	     @Override public void reduce( Text eventID,  Iterable<IntWritable> counts,  Context context)
	         throws IOException,  InterruptedException {

	      int sum  = 0;
	      for ( IntWritable count  : counts) {
	        sum  += count.get();
	      }
	      context.write(eventID,  new IntWritable(sum));
	    }
	}
	
	public static void main(String[] args) throws Exception {
	    if (args.length != 2) {
	      System.err.println("Usage: Frequency <input path> <output path>");
	      System.exit(-1);
	    }

	    // create a Hadoop job and set the main class
	    Job job = Job.getInstance();
	    job.setJarByClass(Frequency.class);
	    job.setJobName("Frequency");

	    // set the input and output path
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    // set the Mapper and Reducer class
	    job.setMapperClass(FrequencyMapper.class);
	    job.setReducerClass(FrequencyReducer.class);

	    // specify the type of the output
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    // run the job
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

}
