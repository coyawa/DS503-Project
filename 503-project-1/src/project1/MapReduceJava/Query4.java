
import java.util.*;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Query4 {

  public static class ReplicateJoinMapper 
      extends Mapper<LongWritable, Text, Text, Text>{
	  
	  private static HashMap<String, String> CustomerMap = new HashMap<String, String>();
	  private BufferedReader brReader;
	  private String countryCode = "";
	  private Text txtMapOutputKey = new Text("");
	  private Text txtMapOutputValue = new Text("");
	  
	  enum MYCOUNTER {
		  RECORD_COUNT, FILE_EXISTS, FILE_NOT_FOUND, SOME_OTHER_ERROR
		  }
	  
	  @Override
	  public void setup(Context context) throws IOException, InterruptedException {
		  Configuration conf = context.getConfiguration();
		  Path[] cacheFilesLocal = DistributedCache.getLocalCacheFiles(conf);
		  
		  loadHashMap(cacheFilesLocal[0], context);
	}
      
	  private void loadHashMap(Path filePath, Context context)
			  throws IOException {
		  
		  String strLineRead = "";
		  
		  try {
			  	brReader = new BufferedReader(new FileReader(filePath.toString()));
			  	// Read each line, split and load to HashMap
			  	while ((strLineRead = brReader.readLine()) != null) {
			  		String deptFieldArray[] = strLineRead.split(",");
			  		CustomerMap.put(deptFieldArray[0].trim(),
			  				deptFieldArray[3].trim());
			  	}
		  } catch (FileNotFoundException e) {
			  e.printStackTrace();
			  context.getCounter(MYCOUNTER.FILE_NOT_FOUND).increment(1);
		  } catch (IOException e) {
			  context.getCounter(MYCOUNTER.SOME_OTHER_ERROR).increment(1);
			  e.printStackTrace();
		  }finally {
			  if (brReader != null) {
				  	brReader.close();
			  }
		  }
	  }
	  
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	if (value.toString().length() > 0) {
    		String arrTraAttributes[] = value.toString().split(",");
    		
    		try{
    			countryCode = CustomerMap.get(arrTraAttributes[1].toString());
    		}finally {
    			countryCode = ((countryCode.equals(null) || countryCode.equals("")) ? "NOT-FOUND" : countryCode);
    		}
    		txtMapOutputKey.set(countryCode);
    		
    		txtMapOutputValue.set(arrTraAttributes[1].toString() + "\t" + arrTraAttributes[2].toString());
    	}
    	context.write(txtMapOutputKey, txtMapOutputValue);
    	countryCode = "";
   }
 }
  
  public static class Reduce 
       extends Reducer<Text,Text,Text,Text> {
    
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      Set uniqCusId = new HashSet();
      float min = 1000;
      float max = 10;
      int count = 0;
      for (Text val : values) {
    	 String[] tokens = val.toString().split("\t");
    	if(!uniqCusId.contains(tokens[0])){
    		count += 1;
    		uniqCusId.add(tokens[0]);
    	}
    	if(Float.valueOf(tokens[1])<min) min = Float.valueOf(tokens[1]);
    	if(Float.valueOf(tokens[1])>max) max = Float.valueOf(tokens[1]);
      }
      context.write(key, new Text(Integer.toString(count)+"\t"+Float.toString(min)+"\t"+Float.toString(max)));
    }
  }

  public static class DriverMapSideJoinDCacheTxtFile extends Configured implements Tool {
	  public int run(String[] args) throws Exception {
		  if (args.length != 3) {
		      System.err.println("Usage: Query4 <HDFS input file> <HDFS output file>");
		      System.exit(2);
		    }
		  	Job job = new Job(getConf());
		    Configuration conf = job.getConfiguration();
		    //Job job = new Job(conf, "customer query");
		    DistributedCache.addCacheFile(new URI(args[0]),conf);
		    job.setJarByClass(Query4.class);
		    job.setMapperClass(ReplicateJoinMapper.class);
		    //job.setCombinerClass(Combine.class);
		    job.setReducerClass(Reduce.class);
		    
		    job.setOutputKeyClass(Text.class);
		    //job.setNumReduceTasks(2);
		    
		    job.setOutputValueClass(Text.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[1]));
		    FileOutputFormat.setOutputPath(job, new Path(args[2]));
		    boolean success = job.waitForCompletion(true);
		    return success ? 0 : 1;
	  }
  }
  
  public static void main(String[] args) throws Exception {
    //Configuration conf = new Configuration();
	  int exitCode = ToolRunner.run(new Configuration(),
			  new DriverMapSideJoinDCacheTxtFile(), args);
	  System.exit(exitCode);
  }
}
