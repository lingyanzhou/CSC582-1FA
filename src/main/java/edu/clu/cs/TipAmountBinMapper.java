package edu.clu.cs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TipAmountBinMapper extends
		Mapper<LongWritable, TripDataTuple, Text, IntWritable> {
	private static final float s_maxTipAmount=20;
	private static final float s_step = 0.1f;
	private Map<Text, IntWritable> m_map = new HashMap<Text, IntWritable>();
	private static Text[] s_keys = new Text[(int) (s_maxTipAmount/s_step+1)];
	
	static {
		for (int i= 0; i<s_keys.length; ++i) {
			s_keys[i] = new Text(Float.toString(quantizeTipAmount(i*s_step)));
		}
	}

	@Override
	public void setup(Context context) {
		m_map.clear();
	}

	@Override
	public void map(LongWritable key, TripDataTuple value, Context context)
			throws IOException, InterruptedException {
		float tipAmount = value.getTipAmount();
		if (value.getPaymentType() != TripDataFormatHelper.s_paymentTypeFactor
				.convertToFloat("CSH") && tipAmount < s_maxTipAmount) {

			Text newKey = s_keys[getIndex(tipAmount)];
			if (m_map.containsKey(newKey)) {
				IntWritable val = m_map.get(newKey);
				val.set(val.get()+1);
			} else {
				m_map.put(newKey, new IntWritable(1));
			}
		}
	}
	
	private int getIndex(float val) {
		return Math.round((val-s_step/2)/s_step);
	}
	
	private static float quantizeTipAmount(float val) {
		return (float) (Math.round((val-s_step/2)/s_step)*s_step);
	}

	@Override
	public void cleanup(Context context) throws IOException,
			InterruptedException {
		for (Text key : m_map.keySet()) {
			context.write(key, m_map.get(key));
		}
	}
}
