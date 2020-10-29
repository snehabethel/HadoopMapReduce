
import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

//Create comparable 
class PairOfFriends implements Comparable<PairOfFriends>{
    public String key;
    public int value;

    PairOfFriends (String key, Integer value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int compareTo(PairOfFriends o) {
        return Integer.compare(o.value, this.value);
    }
}

public class MaxAgeOfFriends {
	
	// Stores the User data file
    private static final String USERDATA = "USERDATA";

    public static class AgeMapper extends Mapper<LongWritable, Text, Text, Text> {
        static HashMap<Integer, String> userDetailsMap = new HashMap<Integer, String>();

		//set up by reading user data file
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            // Extract configuration details
            Configuration config = context.getConfiguration();
            FileSystem filesystem = FileSystem.get(config);
            FileStatus[] fileStatusList = filesystem.listStatus(new Path(config.get(USERDATA)));
            for (FileStatus status : fileStatusList) {
                BufferedReader br = new BufferedReader(new InputStreamReader(filesystem.open(status.getPath())));
                String eachLine = br.readLine();
                while (eachLine != null) {
                    String[] userDetail = eachLine.split(",");
                    // Extract user ID and birthday
                    if (userDetail.length == 10) {
                        userDetailsMap.put(Integer.parseInt(userDetail[0]),userDetail[9]);
                    }
                    eachLine = br.readLine();
                }
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String eachLine = value.toString();

            String[] splitLine = eachLine.split("\t");
            if (splitLine.length == 2) {
            	// Get user and list of friends for each user
                Text mainFriend = new Text(splitLine[0]);
                Text listOfFriends = new Text(splitLine[1]);
                context.write(mainFriend, listOfFriends);
            }
        }
    }

    public static class MaxAgeReducer extends Reducer<Text, Text, Text, Text>{
        static HashMap<Integer, Integer> userDetailsMap = new HashMap<Integer, Integer>();
        // Format in which birthday is stored
        SimpleDateFormat sdf = new SimpleDateFormat("M/d/yyyy");
        // Format in which birthday is to be stored
        DateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration config = context.getConfiguration();
            FileSystem filesystem = FileSystem.get(config);
            FileStatus[] fileStatusList = filesystem.listStatus(new Path(config.get(USERDATA)));
            // Extract the birthday of each user and calculate age
            for (FileStatus status : fileStatusList) {
                BufferedReader br = new BufferedReader(new InputStreamReader(filesystem.open(status.getPath())));
                String eachLine = br.readLine();
                while (eachLine != null) {
                    String[] userDetail = eachLine.split(",");
                    if (userDetail.length == 10) {
                        int age = 0;
                        try {
                            age = calculateAge(userDetail[9]);
                        } catch (ParseException e) {
                            e.printStackTrace();
                        }
                        userDetailsMap.put(Integer.parseInt(userDetail[0]), age);
                    }
                    eachLine = br.readLine();
                }
            }
        }
		
		//calculates age based on current date and the birthday provided in the file
        public int calculateAge(String dateStr) throws ParseException {

            int eachDate = Integer.parseInt(formatter.format(sdf.parse(dateStr)));
            int todayDate = Integer.parseInt(formatter.format(new Date()));
            int age = (todayDate - eachDate) / 10000;
            return age;
        }

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Determine the max age of friends of each user
        	int maxAge = Integer.MIN_VALUE;
            for (Text friendsListStr: values) {
                String[] friendsList = friendsListStr.toString().split(",");
                for (String friend: friendsList){
                    int friendAge = userDetailsMap.get(Integer.parseInt(friend));
                    if (friendAge > maxAge) {
                        maxAge = friendAge;
                    }
                }
            }
            context.write(key, new Text(Integer.toString(maxAge)));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration config = new Configuration();
        String[] remainingArgs = new GenericOptionsParser(config, args).getRemainingArgs();

        config.set(USERDATA, remainingArgs[1]);

        Job job = Job.getInstance(config);

        job.setJarByClass(MaxAgeOfFriends.class);
        job.setMapperClass(AgeMapper.class);
        job.setReducerClass(MaxAgeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(remainingArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(remainingArgs[2]));
        
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}