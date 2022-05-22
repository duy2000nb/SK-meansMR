import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class SReducer extends Reducer<IntWritable, PointWritable, NullWritable, Text> {
	@Override
	protected void reduce(IntWritable id, Iterable<PointWritable> arrPoint, Context context)
			throws IOException, InterruptedException {
		Text text = new Text();
		NullWritable nullWritable = NullWritable.get();
		while(arrPoint.iterator().hasNext()) {
			text.set(arrPoint.iterator().next().toString());
			context.write(nullWritable, text);
		}
	}
}
