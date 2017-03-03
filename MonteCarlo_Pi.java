
 import java.io.IOException; import java.util.*;
import java.lang.Object;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
public class PiValue {
public static class Map extends Mapper<LongWritable, Text, Text,
IntWritable>
{
private final static IntWritable one = new IntWritable(1);
private static int x_val = 0;
private static int y_val = 0;
private static int Result = 0;
private static final int radius = 3;
private Text word = new Text();
public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException
		{
		String line = value.toString();
		StringTokenizer tokenizer = new StringTokenizer(line,",");
		while (tokenizer.hasMoreTokens()) {
		x_val=Integer.parseInt(tokenizer.nextToken());
		y_val=Integer.parseInt(tokenizer.nextToken());
		}
		Result =((x_val-radius)*(x_val-radius))+((y_val-radius)*(y_val-radius));
		if ( radius > Result){
		word.set("Inside");
		}else{
		word.set("Outside");
		}
		context.write(word, one);
		}
		}
		public static class Reduce extends Reducer<Text, IntWritable,
		Text, IntWritable>
		{
		public void reduce(Text key, Iterable<IntWritable> values,
		Context context) throws IOException, InterruptedException
		{
		int sumOfPoints = 0;
		for (IntWritable val : values) {
		sumOfPoints += val.get();
		}
		context.write(key, new IntWritable(sumOfPoints));
		}
		}
		public static void main(String[] args) throws Exception
		{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "WordCount");
		job.setJarByClass(PiValue.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		}
		}