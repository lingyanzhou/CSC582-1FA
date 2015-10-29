package edu.clu.cs;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class NumericalSummaryReducer extends
		Reducer<Text, FloatWritable, Text, NumericalSummaryTuple> {
	private NumericalSummaryTuple result = new NumericalSummaryTuple();

	public void reduce(Text key, Iterable<FloatWritable> values, Context context)
			throws IOException, InterruptedException {
		long totalCount = 0;
		long missingCount = 0;
		double totalVal = 0;
		float min = Float.POSITIVE_INFINITY;
		float max = Float.NEGATIVE_INFINITY; 
		double sumOfSquaredVal = 0;
		for (FloatWritable value : values) {
			float f = value.get();
			if (!Float.isNaN(f)) {
				totalVal += f;
				sumOfSquaredVal += Math.pow(f, 2);
				
				if (f < min) {
					min = f;
				}
				
				if (f > max) {
					max = f;
				}
			} else {
				++missingCount;
			}
			++totalCount;
			
		}
		
		result.setMin(min);
		result.setMax(max);
		float mean = (float)(totalVal / (totalCount - missingCount));
		result.setMean(mean);
		result.setMissingCount(missingCount);
		result.setTotalCount(totalCount);
		
		
		float stdDev = 0;
		stdDev = (float) Math.sqrt(sumOfSquaredVal / (totalCount - missingCount) - Math.pow(mean, 2));
		result.setStdDev(stdDev);
		context.write(key, result);
	}
}
