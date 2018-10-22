package org.phdhy562;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.KeyValueTextInputFormat;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Ex2 extends Configured implements Tool {

	  private static final Logger LOG = Logger.getLogger(Ex2.class);
	  
	  public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Ex2(), args);
	    System.exit(res);
	  }

	  public int run(String[] args) throws Exception {
		  
		JobControl jobControl = new JobControl("jobchain");
		
		Configuration conf = getConf();
		
		ControlledJob contrJob = new ControlledJob(conf);
		
	    Job job1 = Job.getInstance(conf, "wordcount");
	    job1.setJarByClass(this.getClass());
	    
	    FileInputFormat.addInputPath(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(args[1]+ "/tempinput"));
	    job1.setMapperClass(Map.class);
	    job1.setCombinerClass(Reduce.class);
	    
	    job1.setReducerClass(Reduce.class);
	    //EX2 Start
	    job1.setNumReduceTasks(10);
	    FileOutputFormat.setCompressOutput(job1, true);
	    FileOutputFormat.setOutputCompressorClass(job1, GzipCodec.class);
	    //EX2 End
	    
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(IntWritable.class);
	    contrJob.setJob(job1);
	    jobControl.addJob(contrJob);
	    
	    Configuration conf2 = getConf();
		ControlledJob contrJob2 = new ControlledJob(conf2);

	    Job job2 = Job.getInstance(getConf(), "stopwords");
	    job2.setJarByClass(this.getClass());
	    FileInputFormat.addInputPath(job2, new Path(args[1]+ "/tempinput"));
	    FileOutputFormat.setOutputPath(job2, new Path(args[1]+ "/frequency"));
	    
	    job2.setMapperClass(Map2.class);
	    
	    job2.setCombinerClass(Reduce2.class);
	    job2.setReducerClass(Reduce2.class);
	    
	    //EX2 Start
	    job2.setNumReduceTasks(10);
	    FileOutputFormat.setCompressOutput(job2, true);
	    FileOutputFormat.setOutputCompressorClass(job2, GzipCodec.class);
	    //EX2 End
	    
	    job2.setOutputKeyClass(IntWritable.class);
	    job2.setOutputValueClass(Text.class);
	    
	    contrJob2.setJob(job2);
	    contrJob2.addDependingJob(contrJob);

	    jobControl.addJob(contrJob2);
	    
	    Thread jobConThread = new Thread(jobControl);
	    jobConThread.start();
	    while(!jobControl.allFinished()){
	    	try{
	    		Thread.sleep(1000);
	    	} catch (Exception e){
	    		
	    	}
	    }
	    System.exit(0);
	    return (job1.waitForCompletion(true) ? 0 : 1);
	  }

	  public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	    private final static IntWritable one = new IntWritable(1);
	    private Text word = new Text();
	    private boolean caseSensitive = false;
	    
	    private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");
	    
	    protected void setup(Mapper.Context context)
	      throws IOException,
	        InterruptedException {
	      Configuration config = context.getConfiguration();
	      this.caseSensitive = config.getBoolean("wordcount.case.sensitive", false);
	    }
	    @Override
	    public void map(LongWritable offset, Text lineText, Context context)
	        throws IOException, InterruptedException {
	      String line = lineText.toString();
	      if (!caseSensitive) {
	        line = line.toLowerCase();
	      }
	      Text currentWord = new Text();
	        for (String word : WORD_BOUNDARY.split(line)) {
	          if (word.isEmpty()) {
	            continue;
	          }
	          word = word.replaceAll("[^A-Za-z0-9]","");
	          currentWord = new Text(word);
	          context.write(currentWord,one);
	        }         
	      }
	  }
	  
	  public static class Map2 extends Mapper<LongWritable, Text, IntWritable,Text> {
		    private final static IntWritable one = new IntWritable(1);
		    private Text finalWord = new Text();
		    @Override
		    public void map(LongWritable offset, Text lineText, Context context)
		        throws IOException, InterruptedException {
		     
		    	String line = lineText.toString();
		    	String[] splitted = line.split("\\s+");
		    	
		    	int wordFrequency = Integer.parseInt(splitted[1].toString());
		    	if(wordFrequency>4000)
		    	{
		    		one.set(wordFrequency);
		    		finalWord.set(splitted[0].toString());
		    		context.write(one,finalWord);
		    	}
		      }
		  }

	  public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	    @Override
	    public void reduce(Text word, Iterable<IntWritable> counts, Context context)
	        throws IOException, InterruptedException {
	      int sum = 0;
	      for (IntWritable count : counts) {
	        sum += count.get();
	      }
	      context.write(word, new IntWritable(sum));
	    }
	  }
	  
	  public static class Reduce2 extends Reducer<IntWritable,Text, IntWritable, Text> {
		  
		  Text word = new Text();
		  @Override  
		  public void reduce(IntWritable key, Iterable<Text> words, Context context)
		        throws IOException, InterruptedException {
		    for (Text value: words)
		    {
			  word.set(value);
		      context.write(key,word);
		    }
		  }
	  }
}
