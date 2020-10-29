import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

// Create a class to compare the pair of friends for sorting in descending order
class PairOfFriends implements Comparable<PairOfFriends>{
    public String key;
    public Integer value;

    PairOfFriends(String  k, Integer v){
        this.key = k;
        this.value = v;
    }

    @Override
    public int compareTo(PairOfFriends pof) {
        
        return Integer.compare(pof.value, this.value);
    }
}

public class MaxCommonFriends {
    public static class MutualFriendsCountMap extends Mapper<LongWritable, Text, Text, Text> {
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

    public static class MutualFriendsCountReduce extends Reducer<Text, Text, Text, IntWritable>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //Store a map of each friend and the count of friends
        	HashMap<String, Integer> map = new HashMap<String, Integer>();
            int mutualFriendsCount = 0;
            for (Text friends: values) {
            	//Extract the list of mutual friends for each pair
                String[] friendsList = friends.toString().split(",");
                //For each pair of friend in the mutual friend list
                for (String friend: friendsList){
                	// If map contains the friend already, increment the count
                    if (map.containsKey(friend)) {
                        mutualFriendsCount += map.get(friend);
                        map.put(friend, mutualFriendsCount);
                    }
                    // If map doesn't contain the friend, add a new entry to the map
                    else {
                        map.put(friend, 1);
                    }
                }
            }
            //write the output into the context
            context.write(key, new IntWritable(mutualFriendsCount));
        }
    }
    
    // Mapper to put count in descending order
    public static class MutualFriendsCountInDescendingMap extends Mapper<LongWritable, Text, IntWritable, Text> {
        private final static IntWritable constantKey = new IntWritable(1);
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            {
                context.write(constantKey, value);
            }
        }
    }

    //Reducer to put count in descending order
    public static class MutualFriendsCountInDescendingReduce extends Reducer<IntWritable, Text, Text, IntWritable> {
        public  void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            PriorityQueue<PairOfFriends> pQueue = new PriorityQueue<PairOfFriends>();

            for (Text eachLine : values) {
                String[] fields = eachLine.toString().split("\t");
                if (fields.length == 2) {
                    pQueue.add(new PairOfFriends(fields[0], Integer.parseInt(fields[1])));
                }
            }
            
            PairOfFriends p = pQueue.remove();
            context.write(new Text(p.key), new IntWritable(p.value));
            int flag = 1;
            while(flag == 1) {
            	 PairOfFriends q = pQueue.remove();
            	 if(q.value.equals(p.value)) {
            		 context.write(new Text(q.key), new IntWritable(q.value));
            	 }
            	 else {
            		 flag = 0;
            		 break;
            	 }
                 
            	 
            }
			/*
			 * //poll PQ only until we get the top 10 
			 * for (int i = 0; i < 10; i++) {
			 * PairOfFriends p = pQueue.poll(); context.write(new Text(p.key), new
			 * IntWritable(p.value)); }
			 */
        }
    }

    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        //create the first job
        Job first = new Job(conf, "MutualFriendQ2");
        first.setJarByClass(MaxCommonFriends.class);
        first.setMapperClass(MutualFriendsCountMap.class);
        first.setReducerClass(MutualFriendsCountReduce.class);

        // set output key type
        first.setOutputKeyClass(Text.class);
        // set output value type
        first.setOutputValueClass(Text.class);
        //set the HDFS path of the input data
        FileInputFormat.addInputPath(first, new Path(otherArgs[0]));
        // set the HDFS path for the output
        FileOutputFormat.setOutputPath(first, new Path(otherArgs[1]));

        if (first.waitForCompletion(true)) {
            //chain second job to output of first job 
            Configuration secondJobConf = new Configuration();
            Job second = Job.getInstance(secondJobConf);

            second.setJarByClass(MaxCommonFriends.class);
            second.setMapperClass(MutualFriendsCountInDescendingMap.class);
            second.setReducerClass(MutualFriendsCountInDescendingReduce.class);
            second.setInputFormatClass(TextInputFormat.class);

            second.setMapOutputKeyClass(IntWritable.class);
            second.setMapOutputValueClass(Text.class);

            second.setOutputKeyClass(Text.class);
            second.setOutputValueClass(IntWritable.class);

            // Setting input of the second job as output of the first
            FileInputFormat.addInputPath(second, new Path(otherArgs[1]));
            FileOutputFormat.setOutputPath(second, new Path(otherArgs[2]));

            System.exit(second.waitForCompletion(true) ? 0 : 1);
        }
    }
}