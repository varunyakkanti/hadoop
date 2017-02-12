package wtf;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class wtf1 {

    
    /********************/
    /**    Mapper      **/
    /********************/
    
    public static class AllPairsMapper extends Mapper<Object,Text, IntWritable, IntWritable> {
        // Emits (a,b) *and* (b,a) any time a friend common to a and b is found.
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            // Key is ignored as it only stores the offset of the line in the text file
            StringTokenizer st = new StringTokenizer(values.toString());
            // seenFriends will store the friends we've already seen as we walk through the list of friends
            ArrayList<Integer> seenFriends = new ArrayList<Integer>(); 
            // friend1 and friend2 will be the elements in the emitted pairs.
            IntWritable friend1 = new IntWritable();
            IntWritable friend2 = new IntWritable();
            friend1.set(Integer.parseInt(st.nextToken()));
            //st.nextToken(); // discards first token (key)
            while (st.hasMoreTokens()) {
                // For every friend Fi found in the values,
                // we emit (Fi,Fj) and (Fj,Fi) for every Fj in the 
                // friends we have seen before. You can convince yourself
                // that this will emit all (Fi,Fj) pairs for i!=j.
                
            	friend2.set(Integer.parseInt(st.nextToken()));
                    context.write(friend2, friend1);
                    
              System.out.println(friend2+"  "+friend1);
            }
        }
    }
    public static class AllPairsMapper1 extends Mapper<Object, Text, IntWritable, IntWritable> {
        // Emits (a,b) *and* (b,a) any time a friend common to a and b is found.
    	public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            // Key is ignored as it only stores the offset of the line in the text file
            StringTokenizer st = new StringTokenizer(values.toString());
            // seenFriends will store the friends we've already seen as we walk through the list of friends
            ArrayList<Integer> seenFriends = new ArrayList<Integer>(); 
            ArrayList<Integer> existingfrens = new ArrayList<Integer>(); 

            // friend1 and friend2 will be the elements in the emitted pairs.
            IntWritable friend1 = new IntWritable();
            IntWritable friend2 = new IntWritable();
            IntWritable tempfren = new IntWritable();
            //st.nextToken(); // discards first token (key)
            
            IntWritable key1 = new IntWritable();
            key1.set(Integer.parseInt(st.nextToken()));
            while (st.hasMoreTokens()) {
                // If already following a friend ,it is flagged by -ve.
           
                friend1.set(Integer.parseInt(st.nextToken()));
            	if (friend1.get() < 0 )  {
            		existingfrens.add(friend1.get());
            		context.write(key1, friend1);
            		System.out.println("Neg"+key1+","+friend1);
            	} 
            	else {
            		// For every friend Fi found in the values,
                    // we emit (Fi,Fj) and (Fj,Fi) for every Fj in the 
                    // friends we have seen before. You can convince yourself
                    // that this will emit all (Fi,Fj) pairs for i!=j.
            		for (Integer seenFriend : seenFriends) {
                        friend2.set(seenFriend);
                        context.write(friend1, friend2);
                        context.write(friend2, friend1);
                        System.out.println("R"+friend1+","+friend2);
                        System.out.println("R"+friend2+","+friend1);
                    }
                    seenFriends.add(friend1.get());
            		
            	}
                
            }
        }
    }

    /**********************/
    /**      Reducer     **/
    /**********************/
    
    public static class CountReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        
        // The reduce method
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // user stores the id of the user for which we are searching for recommendations
            IntWritable user = key;
            // recommendations will store all the recommendations for user 'user'
            ArrayList<Integer> recommendations = new ArrayList<Integer>();
            // Builds the recommendation array
            while (values.iterator().hasNext()) {
                int userWithCommonFriend = values.iterator().next().get();
               
                    recommendations.add(userWithCommonFriend);
               
            
        }
            String str = recommendations.toString();
            Text t1=new Text(str);
            System.out.println("----------------------recommendations--------------------------");
            context.write(user, t1);
    }
    }
    public static class CountReducer1 extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

        // A private class to describe a recommendation.
        // A recommendation has a friend id and a number of friends in common.
        private static class Recommendation {
            
            // Attributes
            private int friendId;
            private int nCommonFriends;
            
            // Constructor
            public Recommendation(int friendId) {
                this.friendId = friendId;
                // A recommendation must have at least 1 common friend
                this.nCommonFriends = 1;
            }
            // Getters
            public int getFriendId() {
                return friendId;
            }
            public int getNCommonFriends() {
                return nCommonFriends;
            }

            // Other methods
            // Increments the number of common friends
            public void addCommonFriend() {
                nCommonFriends++;
            }
            // String representation used in the reduce output            
            public String toString() {
                return friendId+"("+nCommonFriends+")";
            }
            // Finds a representation in an array
            public static Recommendation find(int friendId, ArrayList<Recommendation> recommendations) {
                for (Recommendation p : recommendations)
                    if (p.getFriendId() == friendId)
                        return p;
                // Recommendation was not found!
                return null;
            }
        }

        // The reduce method
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            // user stores the id of the user for which we are searching for recommendations
            IntWritable user = key;
            // recommendations will store all the recommendations for user 'user'
            ArrayList<Recommendation> recommendations = new ArrayList<Recommendation>();
            // Builds the recommendation array
            while (values.iterator().hasNext()) {
                int userWithCommonFriend = values.iterator().next().get();
                Recommendation p = Recommendation.find(userWithCommonFriend, recommendations);
                if (p == null)
                    // no recommendation exists for user 'userWithCommonFriend'. Let's create one.
                    recommendations.add(new Recommendation(userWithCommonFriend));
                else
                    // there is already a recommendation for user 'userWithCommonFriend'. Let;s
                    // increment the number of friends in common.
                    p.addCommonFriend();
                System.out.println("mdmhfblskjdnvljsjvsljv");
            }
            // Sorts the recommendation array by number of common friends
            // See javadoc on Comparator at https://docs.oracle.com/javase/8/docs/api/java/util/Comparator.html
            recommendations.sort(new Comparator<Recommendation>() {
                public int compare(Recommendation t, Recommendation t1) {
                    return -Integer.compare(t.getNCommonFriends(), t1.getNCommonFriends());
                }
            });
            // Builds the output string that will be emitted
            StringBuffer sb = new StringBuffer(""); // Using a StringBuffer is more efficient than concatenating strings
            for (int i = 0; i < recommendations.size() && i < 10; i++) {
                Recommendation p = recommendations.get(i);
                sb.append(p.toString()+" ");
            }
            Text result = new Text(sb.toString());
            context.write(user, result);
        }
    }
    

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "people you may know");
        job.setJarByClass(wtf1.class);
        job.setMapperClass(AllPairsMapper.class);
        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
        
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf, "people you may know");
        job1.setJarByClass(wtf1.class);
        job1.setMapperClass(AllPairsMapper1.class);
        job1.setReducerClass(CountReducer1.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[2]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        
        System.exit(job1.waitForCompletion(true) ? 0 : 1);
    }

}
	
	
