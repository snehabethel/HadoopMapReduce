import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MutualFriendsDetails {
	
	//Store user details, IDs of friend1 and friend2 in strings from input
    private static final String FRIEND1 = "FRIEND1";
    private static final String FRIEND2 = "FRIEND2";
    private static final String USERDATA = "USERDATA";

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        //Friend Pair Output of the mapper 
    	private Text friendPair = new Text();
    	// Store user details in a hashmap
        static HashMap<Integer, String> userDetailsMap = new HashMap<Integer, String>();

		// Read the input file and create hash map of all user (required) details
        protected void setup(Context context) throws IOException, InterruptedException {
			// Extract details from context
            super.setup(context);
            Configuration config = context.getConfiguration();
            FileSystem fileSystem = FileSystem.get(config);
            // Get User Details File into a list
            FileStatus[] fileStatusList = fileSystem.listStatus(new Path(config.get(USERDATA)));
            for (FileStatus status : fileStatusList) {
                BufferedReader br = new BufferedReader(new InputStreamReader(fileSystem.open(status.getPath())));
                // Read each line of user details file
                String eachLine = br.readLine();
                while (eachLine != null) {
                    String[] infoList = eachLine.split(",");
                    // Check if user details is valid
                    if (infoList.length == 10) {
                    	// Extract ID, Name and Date of Birth of each user
                        userDetailsMap.put(Integer.parseInt(infoList[0]), infoList[1] + ":" + infoList[9]);
                    }
                    eachLine = br.readLine();
                }
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //Extract each line from value
        	String eachLine = value.toString();
            String[] spliteachLine = eachLine.split("\t");
            Configuration config = context.getConfiguration();
            // Identify the details of the input friend 1 and friend 2
            int inputFriend1 = Integer.parseInt(config.get(FRIEND1));
            int inputFriend2 = Integer.parseInt(config.get(FRIEND2));
            // If the input is valid
            if (spliteachLine.length == 2) {
            	// Identify each pair of friends and compare them to input pair
                int tempFriend1 = Integer.parseInt(spliteachLine[0]);
                String[] friendList = spliteachLine[1].split(",");
                for (int i = 0; i < friendList.length; i++) {
                    int tempFriend2 = Integer.parseInt(friendList[i]);
                    // Comparison
                    if ((tempFriend2 == inputFriend1 && tempFriend1 == inputFriend2) || (tempFriend1 == inputFriend1 && tempFriend2 == inputFriend2)) {
                        StringBuffer sb = new StringBuffer();
                        // Reordering based on ascending order of IDs
                        if (tempFriend1< tempFriend2) {
                            friendPair.set(tempFriend1 + "," + tempFriend2);
                        }
                        else {
                            friendPair.set(tempFriend2+ "," + tempFriend1);
                        }
                        for (int j = 0; j < friendList.length; j++) {
                        	// Extract the ID 
                            int friendID = Integer.parseInt(friendList[j]);
                            // Add friend ID to output
                            sb.append(friendID + ":" + userDetailsMap.get(friendID) + ", ");
                        }
                        //Remove trailing commas
                        if (sb.lastIndexOf(",") > -1) {
                            sb.deleteCharAt(sb.lastIndexOf(","));
                        }

                        context.write(friendPair, new Text(sb.toString()));
                    }
                }
            }

        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text>{
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // create a hash set
        	Set<String> hs = new HashSet<String>();
        	// Store final user details in a string
            StringBuilder mutualFriendsDetailsString = new StringBuilder();
            mutualFriendsDetailsString.append("[");
            for (Text friendDetailsList : values) {
                String[] friendsList = friendDetailsList.toString().split(",");
                for (String f: friendsList) {
                	// if hash set contains friend, add his details
                    if (hs.contains(f)) {
                        String[] userDetail = f.split(":");
                        mutualFriendsDetailsString.append(userDetail[1]+":"+userDetail[2]+ ",");
                    } // If hash set doesn't contain friend yet, add to it 
                    else {
                        hs.add(f);
                    }
                }
            }
            // Remove trailing commas
            if (mutualFriendsDetailsString.lastIndexOf(",") > -1) {
            	mutualFriendsDetailsString.deleteCharAt(mutualFriendsDetailsString.lastIndexOf(","));
            }
            mutualFriendsDetailsString.append("]");
            // write to output context
            context.write(key, new Text(mutualFriendsDetailsString.toString()));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Provide Input in the format - 
        // <Input_Path_Mutual_Friends> <Input_Path_For_User_Data> 
        				//<Friend1_ID> <Friend2_ID> <Output_Path>
        conf.set(USERDATA, remainingArgs[1]);
        conf.set(FRIEND1, remainingArgs[2]);
        conf.set(FRIEND2, remainingArgs[3]);

        Job job = Job.getInstance(conf);

        job.setJarByClass(MutualFriendsDetails.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[4]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}