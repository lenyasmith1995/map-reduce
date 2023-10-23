package mapreducepackage;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JoinReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>{
	
	  @Override
	  protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, DoubleWritable>.Context context) throws IOException, InterruptedException {
		
		double avaragerating = Double.MIN_VALUE;
		int count = 0;
		double summ = 0.0;
		
	    for (IntWritable value : values) {
	    	summ = Double.sum(summ, value.get());
	    	count = Integer.sum(count, 1); 
	    }
	    avaragerating = summ/count;
	    
	    context.write(key, new DoubleWritable(Double.valueOf(avaragerating)));
	    
	  }

}
