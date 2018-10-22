package org.phdhy562;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class Ex3 extends Configured implements Tool {

	  private static final Logger LOG = Logger.getLogger(Ex3.class);
	  
	  public static void main(String[] args) throws Exception {
	    int res = ToolRunner.run(new Ex3(), args);
	    System.exit(res);
	  }

	  public int run(String[] args) throws Exception {
		  
		JobControl jobControl = new JobControl("jobchain");
		
		Configuration conf = getConf();
		
		ControlledJob contrJob = new ControlledJob(conf);
		
	    Job job1 = Job.getInstance(conf, "wordcount");
	    job1.setJarByClass(this.getClass());
	    
	    FileInputFormat.addInputPath(job1, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job1, new Path(args[1]+ "/tempOutput"));
	    job1.setMapperClass(Map.class);
	    job1.setCombinerClass(Reduce.class);
	    
	    job1.setReducerClass(Reduce.class);
	    
	    job1.setOutputKeyClass(Text.class);
	    job1.setOutputValueClass(Text.class);
	    contrJob.setJob(job1);
	    jobControl.addJob(contrJob);
	    
	    Configuration conf2 = getConf();
		ControlledJob contrJob2 = new ControlledJob(conf2);

	    Job job2 = Job.getInstance(getConf(), "stopwords");
	    job2.setJarByClass(this.getClass());
	    FileInputFormat.addInputPath(job2, new Path(args[1]+ "/tempinput"));
	    FileOutputFormat.setOutputPath(job2, new Path(args[1]+ "/frequency"));
	    
	    return (job1.waitForCompletion(true) ? 0 : 1);
	  }

	  public static class Map extends Mapper<LongWritable, Text, Text, Text> {
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
	      Text filename = new Text();
	      
	      String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
	      filename.set(fileName);
	      
	      Text currentWord = new Text();
	        for (String word : WORD_BOUNDARY.split(line)) {
	          if (word.isEmpty()) {
	            continue;
	          }
	          word = word.replaceAll("[^A-Za-z0-9]","");
	          currentWord = new Text(word);
	          context.write(currentWord,filename);
	        }         
	      }
	  }
	
	  public static class Reduce extends Reducer<Text, Text, Text, Text> {
		  @Override
		    public void reduce(Text word, Iterable<Text> counts, Context context)
		        throws IOException, InterruptedException {
			  
			  Text retValue = new Text();
			  java.util.Map<String, Integer> counter = new HashMap<String, Integer>();
			  for (Text count : counts) {
		      {
		    	  String file = count.toString();
		    	  Integer cur = counter.get(file);
		    	  if(cur==null)
		    	  {
		    		  cur =0;
		    	  }
		    	  counter.put(file, ++cur);
		      }
		      
		      StringBuilder value = new StringBuilder();
		      boolean addComma = true;
		      for(Entry<String, Integer> entry: counter.entrySet())
		      {
		    	  if(!addComma){
		    		  value.append(",");
		    	  }
		    	  addComma=false;
		    	  value.append(entry.getKey()).append("#").append(entry.getValue());
		      }
		      retValue.set(value.toString());
			  context.write(word,retValue);
		    }
		  }
	  }
	 
}
