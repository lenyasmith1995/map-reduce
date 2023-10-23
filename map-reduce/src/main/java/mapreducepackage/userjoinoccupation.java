package mapreducepackage;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class userjoinoccupation implements Writable{
	
	 private int userId;
	 private int rating;
	 private String occupation;

	 public int getuserId() {
	  return userId;
	 }
	 public void setuserId(int userId) {
	  this.userId = userId;
	 }
	 public int getrating() {
	  return rating;
	 }
	 public void setrating(int rating) {
	  this.rating = rating;
	 }
	 public String getoccupation() {
	  return occupation;
	 }
	 public void setoccupation(String occupation) {
	  this.occupation = occupation;
	 }

	 @Override
	 public void write(DataOutput out) throws IOException {
	  out.writeInt(userId);
	  out.writeInt(rating);
	  out.writeUTF(occupation);

	 }
	 @Override
	 public void readFields(DataInput in) throws IOException {
	  userId = in.readInt();
	  rating = in.readInt();
	  occupation = in.readUTF();
	 }

	 @Override
	 public String toString() {
	  return "User_occupation_with_rating [userId=" + userId + ", rating=" + rating + ",   occupation=" + occupation + "]";
	 }

}
