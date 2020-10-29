
import java.io.IOException;
import java.util.*;

//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class FindMutualFriends {

    public static class Map
            extends Mapper<LongWritable, Text, Text, Text>{
    	
    	//determine the output key type
        private Text output = new Text(); 
        private Text outputList = new Text();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //Store each line from input file
        	String[] eachLine = value.toString().split("\t");
            if (eachLine.length == 2) {
                // set user as the first value of each line
                String user = eachLine[0];
                // set the list of friends as the second value of each line
                List<String> listOfFriends = Arrays.asList(eachLine[1].split(","));
                // Iterate over list of friends and compare the value of user and friend
                for (String friend : listOfFriends) {
                	//Set the values such that the lower id is displayed first
                    if (Integer.parseInt(user) < Integer.parseInt(friend))
                        output.set(user + "," + friend);
                    else
                        output.set(friend + "," + user);
                    outputList.set(eachLine[1]);
                    //write output into context
                    context.write(output, outputList);
                }
            }
        }
    }

    public static class Reduce
            extends Reducer<Text,Text,Text,Text> {
    	//Display output for only the required pairs
        private Text requiredResult = new Text();
        //Store set of required pairs
        private Set<String> required= new HashSet(Arrays.asList("0,1", "20,28193", "1,29826", "6222,19272", "28041,28056"));

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //Store a hashmap of friends list
        	HashMap<String, Integer> mapFriendsList = new HashMap<String, Integer>();
            StringBuilder tempString = new StringBuilder();

            for (Text friends :  values) {
                List<String> mutualList = Arrays.asList(friends.toString().split(","));
                for (String friend : mutualList) {
                	// append to string of friends is already present in hashmap
                    if (mapFriendsList.containsKey(friend))
                        tempString.append(friend + ','); 
                    else
                    	// else create a new entry in hashmap
                        mapFriendsList.put(friend, 1);
                }
            }
            // remove trailing ","
            if (tempString.lastIndexOf(",") > -1) {
            	tempString.deleteCharAt(tempString.lastIndexOf(","));
            }
            // Store only required result into context 
            requiredResult.set(new Text(tempString.toString()));
            if(required.contains(key.toString())) {
                context.write(key, requiredResult);
            }
        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      Job job = new Job(conf, "MutualFriendQ1");
        job.setJarByClass(FindMutualFriends.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // set output key type
        job.setOutputKeyClass(Text.class);
        // set output value type
        job.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}