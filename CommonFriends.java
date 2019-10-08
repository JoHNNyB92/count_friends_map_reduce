import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.util.*;
import java.lang.*;
public class CommonFriends {
	public static class PairOfFriends extends Mapper<Object, Text, Text, Text>{
    private Text word = new Text();
	
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    //Line read.
      StringTokenizer itr = new StringTokenizer(value.toString());
      //The first element of the line will contain the user's name.
      Text name=new Text();
      name.set(itr.nextToken());
      ArrayList<String> object = new ArrayList<String>();
      //friends is a string item of the form Fr1,Fr2,Fr3 containing all the friends of user.
      String friends="";
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        friends=friends+","+word.toString();
        object.add(word.toString());
      }
      friends=friends.substring(1);
      //Here we will create every pair consinsting of user name and friend name in lexicographical order. Thus,when the map will emit those into the reducer,
      //the set of friends from two items will go to the same reducer.
      for(String friend:object){
      	String fr=friend.toString();
      	String nm=name.toString();
      	if((fr.compareTo(nm))<0){
      		Text tkey=new Text(fr+','+nm);
      		context.write(tkey,new Text(friends));
      	}
      	else{
      		Text tkey=new Text(nm+","+fr);
      		context.write(tkey,new Text(friends));
      	}
      	
      }
    }
  }

  public static class FindCommonFriends extends Reducer<Text, Text,Text,Text> {
	public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
      int mcase=0;
      String first="";
      String sec="";
      //Each values object will contain the elements of Elem1,Elem2 and Elem2,Elem1(which will be transformed to 
      //Elem1,Elem2 too in order to be processed by the same reducer.First is friends of Elem1, second is friends of Elem2.
      for(Text val:values){
      	if (mcase==0){
      		first=val.toString();
       		mcase=mcase+1;
       }
       else{
       		sec=val.toString();
       } 
      }
      //The values are formed as A,B,C,D. We need to split based on commas and retrieve each different friend per user.
   	  String[] lst1=first.split(",");
      String[] lst2=sec.split(",");
      Arrays.sort(lst2);
      Arrays.sort(lst1);
      String[] cmp1;
      String[] cmp2;
      //cmp1 contains always the smallest list.It is used as outer list, since it has the fewer elements and ,thus, less iterations would be required.
      if(lst1.length>lst2.length){
      	cmp1=lst2;
      	cmp2=lst1;
      }
      else{
      	cmp1=lst1;
      	cmp2=lst2;    
      }
      String common="";
      //String common will contain the common elements between the two users.
      //Since the lists are sorted ,we can use that to limit the for iterations.When the outer element i is bigger (lexikograpically) from the element j the search stops.
      for(String i : cmp1){
      	for(String j : cmp2){
      		if((i.compareTo(j))>=0){
      			if(i.equals(j)==true){
      				common=common+","+i;
      			}
      		}
      	}
      }
      String ret="";
      //Remove the inital ','.
      if(common!=""){
      	ret=common.substring(1);
      }
      System.out.println(key+"--"+ret);
      context.write(key, new Text(ret));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(CommonFriends.class);
    job.setMapperClass(PairOfFriends.class);
    job.setReducerClass(FindCommonFriends.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

