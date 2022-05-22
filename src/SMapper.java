import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class SMapper extends Mapper<LongWritable, Text, IntWritable, PointWritable>{
	private LongWritable id = new LongWritable();
	private int k;
	private float thresholdStop;
	
	@Override
	public void setup(Context context) {
		Configuration conf = context.getConfiguration();
		k = conf.getInt("k", 3);
		thresholdStop = conf.getFloat("thresh", 0.001f);
//		this.currCentroids = new PointWritable[k]; //luu cac tam cum hien tai
//		for (int i = 0; i < k; i++) {
//			String[] centroid = context.getConfiguration().getStrings("C" + i);
//			this.currCentroids[i] = new PointWritable(centroid);
	}
	
	private PointWritable[] initRandomCentroids(int k, int numberOfPoint, PointWritable pointInput[]) {
		List<Integer> indexPos = new ArrayList<Integer>();
		Random random = new Random();
		int pos;
		//Lấy ngẫu nhiên chỉ số dòng của 2*k cụm
		while (indexPos.size() < k*2) {
			pos = random.nextInt(numberOfPoint);
			if (!indexPos.contains(pos)) {
				indexPos.add(pos);
			}
		}
		System.out.println(indexPos.size() + " " + k*2);
		
		PointWritable centroids[] = new PointWritable[k*2];
		for(int i = 0; i < k*2; i++) {
			centroids[i] = pointInput[indexPos.get(i)];
		}
		return centroids;
	}
	
	private PointWritable[] kMean(int k, PointWritable pointInput[], PointWritable[] currCentroids) {
		PointWritable newCentroids[] = currCentroids.clone();
		double minDistance;
		int centroidIdNearest;
		double distance;
		for(int i = 0; i < pointInput.length; i++) {
			//Tính khoảng cách và tìm tâm mới
			minDistance = Double.MAX_VALUE;
			centroidIdNearest = 0; // khoi tao bien luu id tam gan nhat bang 0
			for (int j = 0; j < currCentroids.length; j++) { //duyet het cac tam hien tai de tinh khoang cach diem dau vao den tam
				distance = pointInput[i].calcDistance(currCentroids[j]);
				if (distance < minDistance) {
					centroidIdNearest = j;
					minDistance = distance;
				}
			}
			newCentroids[centroidIdNearest].sum(pointInput[i]);
		}
		for(int i = 0; i < k*2; i++) {
			newCentroids[i].calcAverage();
		}
		return newCentroids;
	}
	
	private boolean checkStopKMean(PointWritable[] oldCentroids, PointWritable[] newCentroids, float thresholdStop) {
		//Kiểm tra tâm mới cách tâm cũ < thresholdStop không
		boolean needStop = true;
		for (int i = 0; i < oldCentroids.length; i++) {
			double dist = oldCentroids[i].calcDistance(newCentroids[i]);
			needStop = dist <= thresholdStop;
			// chi can 1 tam < nguong thi return false
			if (!needStop) {
				return false;
			}
		}
		//Nếu nhỏ hơn thì return true
		return true;
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] arrPoint = value.toString().split(";");
		int numberOfPoint = arrPoint.length;
		PointWritable pointInput[] = new PointWritable[numberOfPoint];
		String[] attributes;
		
		for(int i = 0; i < numberOfPoint; i++) {
			attributes = arrPoint[i].split(",");
			pointInput[i] = new PointWritable(attributes);
		}
		
		PointWritable centroids[] = initRandomCentroids(k, numberOfPoint, pointInput);
		PointWritable newCentroids[];
		while(true) {
			newCentroids = kMean(k, pointInput, centroids);
			if(checkStopKMean(centroids, newCentroids, thresholdStop)) {
				break;
			}
		}
		for(int i = 0; i < k*2; i++) {
			context.write(new IntWritable(1), newCentroids[i]);
		}
	}
}
