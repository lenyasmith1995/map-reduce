package mapreducepackage;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	 private Map<Integer, Integer> UserMap  = new HashMap<Integer, Integer>();
	 private Map<Integer, String> OccupationMap  = new HashMap<Integer, String>();
	 
	 @Override
	 protected void map(LongWritable key, Text value,
	   Mapper<LongWritable, Text, Text, IntWritable>.Context context)
	     throws IOException, InterruptedException {
		   System.out.println("HERE");
		 
		  try {
			  String[] columns = value.toString().split(Pattern.quote("::"),4);
			  if (columns != null && columns.length > 3) {
			   userjoinoccupation ujr = new userjoinoccupation();
			   ujr.setuserId(Integer.parseInt(columns[0]));
			   ujr.setrating(Integer.parseInt(columns[2]));
			   ujr.setoccupation(OccupationMap.get(UserMap.get(ujr.getuserId())));
			   context.write(new Text(ujr.getoccupation()), new IntWritable(ujr.getrating()));
			  }
	      } catch (Exception e) {
	    	  //System.out.println("end");
	      }
	  
	 }

	 @Override
	 protected void setup(Mapper<LongWritable, Text, Text, IntWritable>.Context context)
	   throws IOException, InterruptedException {
	  loadUserInMemory(context);
	 }

	 private void loadUserInMemory(Mapper<LongWritable, Text, Text, IntWritable>.Context context) {
		 	 
		 
		 	String userdictionary = "/home/lenyasmith1995/Documents/MapReduceProject/users.dat";
		 	BufferedReader reader = null;
		    try {
		      reader = new BufferedReader(new FileReader(userdictionary));
		      String line;
				  while ((line = reader.readLine()) != null) {
					  String[] userproperty = line.toString().split(Pattern.quote("::"),5);
					  Integer userid = Integer.valueOf(userproperty[0]);
					  Integer occupationid = Integer.valueOf(userproperty[3]);
				  	  UserMap.put(userid, occupationid);
				  }
		    } catch (IOException f) {
		        System.out.println(userdictionary + " does not exist");
		      } finally {
		          if (reader != null) {
		              try {
		                  reader.close();
		              } catch (IOException e) {
		                  e.printStackTrace();
		              }
		          }
		      }	
		 
		 	String occupationdictionary = "/home/lenyasmith1995/Documents/MapReduceProject/occupation.dat";
		 	BufferedReader br = null;
		 	
		 	try {
			      br = new BufferedReader(new FileReader(occupationdictionary));
			      String line;
					  while ((line = br.readLine()) != null) {
						  String[] occupation = line.toString().split(Pattern.quote(":"),2);
						  Integer occupationid = Integer.valueOf(occupation[0]);
						  String occupationvalue = occupation[1];
					  	  OccupationMap.put(occupationid, occupationvalue);
					  }
			    } catch (IOException f) {
			        System.out.println(occupationdictionary + " does not exist");
			      } finally {
			          if (br != null) {
			              try {
			                  br.close();
			              } catch (IOException e) {
			                  e.printStackTrace();
			              }
			          }
			      }
		 	
		 	
	 }
}
