package wtf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.StringTokenizer;
import java.util.function.Predicate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.Chain;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class wtf1 {

	private static final String OUTPUT_PATH = "intermediate_output42";
    /********************/
    /**    Mapper  2    **/
    /********************/
	    public static class SecondMapper extends Mapper<Object , Text, IntWritable, IntWritable> {
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
	    /********************/
	    /**    Mapper  1    **/
	    /********************/
    public static class FirstMapper extends Mapper<Object, Text, IntWritable, IntWritable> {
         // Inverts the array of input followers to follwed by list
        public void map(Object key, Text values, Context context) throws IOException, InterruptedException {
            // Key is ignored as it only stores the offset of the line in the text file
            StringTokenizer st = new StringTokenizer(values.toString());
            // seenFriends will store the friends we've already seen as we walk through the list of friends
            ArrayList<Integer> seenFriends = new ArrayList<Integer>(); 
           
            IntWritable friend1 = new IntWritable();
            IntWritable key1 = new IntWritable();
 			IntWritable tempfriend = new IntWritable();
            key1.set(Integer.parseInt(st.nextToken())); 
             
             while (st.hasMoreTokens()) {
 				String s =st.nextToken();
                 friend1.set(Integer.parseInt(s));
                 //Emit the list of existing frens as negetive values so it can be ignored in final recomended list
                 tempfriend.set(- Integer.parseInt(s));
                     context.write(friend1,key1);
                     context.write(key1, tempfriend);
                     System.out.println("pos"+key1+","+friend1);
                     System.out.println("Neg"+key1+","+tempfriend);
             }

        }
    }

    /**********************/
    /**      Reducer  2   **/
    /**********************/
    public static class SecondReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

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
                return friendId + "(" + nCommonFriends + ")";
            }

            // Finds a representation in an array
            public static Recommendation find(int friendId, ArrayList<Recommendation> recommendations) {
                for (Recommendation p : recommendations) {
                    if (p.getFriendId() == friendId) {
                        return p;
                    }
                }
                // Recommendation was not found!
                return null;
            }
        }

        // The reduce method       
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            IntWritable user = key;
            // 'existingFriends' will store the friends of user 'user'
            // (the negative values in 'values').
            ArrayList<Integer> existingFriends = new ArrayList<Integer>();
            // 'recommendedUsers' will store the list of user ids recommended
            // to user 'user'
            ArrayList<Integer> recommendedUsers = new ArrayList<Integer>();
            while (values.iterator().hasNext()) {
                int value = values.iterator().next().get();
                if (value > 0) {
                    recommendedUsers.add(value);
               } else {
                   existingFriends.add(Math.abs(value));
               }
            }
            // 'recommendedUsers' now contains all the positive values in 'values'.
            // We need to remove from it every value -x where x is in existingFriends.
            // See javadoc on Predicate: https://docs.oracle.com/javase/8/docs/api/java/util/function/Predicate.html
            //for ( Integer friend : existingFriends) {
                
            recommendedUsers.removeAll(existingFriends);
                
             //   System.out.println("Remove lsit"+friend);
            
            
            ArrayList<Recommendation> recommendations = new ArrayList<Recommendation>();
            // Builds the recommendation array
            for (Integer userId : recommendedUsers) {
                Recommendation p = Recommendation.find(userId, recommendations);
                if (p == null) {
                    recommendations.add(new Recommendation(userId));
                } else {
                    p.addCommonFriend();
                }
            }
            // Sorts the recommendation array
            // See javadoc on Comparator at https://docs.oracle.com/javase/8/docs/api/java/util/Comparator.html
            recommendations.sort(new Comparator<Recommendation>() {
                //@Override
                public int compare(Recommendation t, Recommendation t1) {
                    return -Integer.compare(t.getNCommonFriends(), t1.getNCommonFriends());
                }
            });
            // Builds the output string that will be emitted
            StringBuffer sb = new StringBuffer(""); // Using a StringBuffer is more efficient than concatenating strings
            for (int i = 0; i < recommendations.size() && i < 10; i++) {
                Recommendation p = recommendations.get(i);
                sb.append(p.toString() + " ");
            }
            Text result = new Text(sb.toString());
            context.write(user, result);
        }
    }

    /**********************/
    /**      Reducer  1   **/
    /**********************/
		
    public static class FirstReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {

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
            Text result = null;
            String s = null;
            // recommendations will store all the recommendations for user 'user'
            ArrayList<Recommendation> recommendations = new ArrayList<Recommendation>();
            // Builds the recommendation array
            Integer i = 0;
            while (values.iterator().hasNext()) {
            	Integer sb = values.iterator().next().get();
            	
            	if(i==0) {
            		s =sb.toString();
            		System.out.println("firstiter"+s);
            		
            	}else{
            		s =s+" "+sb.toString();
            		System.out.println("seconiterator"+s);
            	}
            	
            	i++ ;	        	
            }
            System.out.println(s);
            result = new Text(s.toString());
            context.write(user, result);

        }    
        }
		

    public static int main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
    	//First set of mapper/reducer
    	Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "people you may know");
	//	job.setJarByClass(Chain.class);
        job.setJarByClass(wtf1.class);
        job.setMapperClass(FirstMapper.class);
        job.setReducerClass(FirstReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
        job.waitForCompletion(true); ;
		
        //Second set of mapper/reducer
		Configuration conf2 = new Configuration();;
        Job job2 = new Job(conf2, "Job 2");
        job2.setJarByClass(wtf1.class);
        
        job2.setMapperClass(SecondMapper.class);
        job2.setReducerClass(SecondReducer.class);
        
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        
        return job2.waitForCompletion(true) ? 0 : 1;
    }

}