
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

public class Query5 {

  private static HashMap<String, Integer> CustTrans = new HashMap<String, Integer>();
  private static HashMap<String, Integer> CusName = new HashMap<String, Integer>();
  private static HashMap<String, String> CustomerMap = new HashMap<String, String>();
  private static int size_all = 0;
  private static int sum_all = 0;
  
  public static class ReplicateJoinMapper 
      extends Mapper<LongWritable, Text, Text, Text>{
	  
	  //private static HashMap<String, String> CustomerMap = new HashMap<String, String>();
	  private BufferedReader brReader;
	  private String Name = "";
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
			  				deptFieldArray[1].trim());
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
    			Name = CustomerMap.get(arrTraAttributes[1].toString());
    		}finally {
    			Name = ((Name.equals(null) || Name.equals("")) ? "NOT-FOUND" : Name);
    		}
    		if(!CustTrans.containsKey(arrTraAttributes[1].toString())) CustTrans.put(arrTraAttributes[1].toString(),1);
    		else{ 
    			CustTrans.put(arrTraAttributes[1].toString(),CustTrans.get(arrTraAttributes[1].toString())+1);
    		}
    		//avg = Integer.valueOf(arrTraAttributes[1]);
    			
    		txtMapOutputKey.set(arrTraAttributes[1].toString());
    		txtMapOutputValue.set(Name);
    	}
    	context.write(txtMapOutputKey, txtMapOutputValue);
    	Name = "";
   }
      @Override
	  public void cleanup(Context context) throws IOException, InterruptedException {
    	for(Map.Entry<String, Integer> entry:CustTrans.entrySet()){
    		sum_all += entry.getValue();
    		size_all += 1;
    	}
    	context.write(new Text("Average"),new Text(Integer.toString(size_all)+"\t"+Integer.toString(sum_all)));
    }
 }
  
  public static class Reduce 
       extends Reducer<Text,Text,Text,Text> {
	private int target = 0;
	private int merge_freq = 0;
	private int merge_size = 0;
    
	private Text res_name = new Text();
    public void reduce(Text key, Iterable<Text> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      String name = "";
      int ct = 0;
      if(key.toString().equals("Average")){
    	  for (Text val : values) {
    		  String arrAvg[] = val.toString().split("\t");
    		  merge_size += Integer.valueOf(arrAvg[0]);
    		  merge_freq += Integer.valueOf(arrAvg[1]);
    		  ct+=1;
    	  }
    	  merge_size = merge_size/ct;
    	  target = merge_freq/merge_size;
      }else{
    	  for (Text val : values) {
    		  name = name.concat(val.toString());
    		  ct+=1;
    		  break;
    	  }
    	  for (Text val : values) {
    		  ct += 1;
    	  }
      CusName.put(name, ct);
     }
   }
    @Override
	public void cleanup(Context context) throws IOException, InterruptedException {
    	for(Map.Entry<String, Integer> entry:CusName.entrySet()){
    		if(entry.getValue() > target){
    		context.write(new Text(entry.getKey()),new Text(" "));
    		}
    	}	
    }
  }

  public static class DriverMapSideJoinDCacheTxtFile extends Configured implements Tool {
	  public int run(String[] args) throws Exception {
		  if (args.length != 3) {
		      System.err.println("Usage: Query5 <HDFS input file> <HDFS output file>");
		      System.exit(2);
		    }
		  	Job job = new Job(getConf());
		    Configuration conf = job.getConfiguration();
		    DistributedCache.addCacheFile(new URI(args[0]),conf);
		    job.setJarByClass(Query5.class);
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
