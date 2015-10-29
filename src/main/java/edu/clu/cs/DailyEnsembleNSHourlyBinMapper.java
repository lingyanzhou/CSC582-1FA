package edu.clu.cs;

import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;


public class DailyEnsembleNSHourlyBinMapper
extends Mapper<LongWritable, TripDataTuple, Text, FloatWritable> {
	private Text m_key = new Text();
	private FloatWritable m_val = new FloatWritable();
	private Map<String, Float> m_keyCountMap = new HashMap<String, Float>();
	
	@Override
	public void setup(Context context) {
		m_keyCountMap.clear();
	}
			
	@Override
	public void map(LongWritable key, TripDataTuple value, Context context)
			throws IOException, InterruptedException {

		String kstr = "PickupCount-"+Integer.toString(value.getPickupDatetime().get(Calendar.HOUR_OF_DAY));
		Float tmp = m_keyCountMap.get(kstr);
		if (null == tmp) {
			m_keyCountMap.put(kstr, 1.0f);
		} else {
			m_keyCountMap.put(kstr, 1+tmp.floatValue());
		}
		

		kstr = "DropoffCount-"+Integer.toString(value.getDropoffDatetime().get(Calendar.HOUR_OF_DAY));
		tmp = m_keyCountMap.get(kstr);
		if (null == tmp) {
			m_keyCountMap.put(kstr, 1.0f);
		} else {
			m_keyCountMap.put(kstr, 1+tmp.floatValue());
		}
		

		kstr = "DropoffCount-"+Integer.toString(value.getDropoffDatetime().get(Calendar.HOUR_OF_DAY));
		tmp = m_keyCountMap.get(kstr);
		if (null == tmp) {
			m_keyCountMap.put(kstr, value.getTotalAmount());
		} else {
			m_keyCountMap.put(kstr, value.getTotalAmount()+tmp.floatValue());
		}
		
		kstr = "TipAmount-"+Integer.toString(value.getDropoffDatetime().get(Calendar.HOUR_OF_DAY));
		tmp = m_keyCountMap.get(kstr);
		if (null == tmp) {
			m_keyCountMap.put(kstr, value.getTipAmount());
		} else {
			m_keyCountMap.put(kstr, value.getTipAmount()+tmp.floatValue());
		}
	}
	
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		for (String key : m_keyCountMap.keySet()) {
			m_key.set(key);
			m_val.set(m_keyCountMap.get(key).intValue());
			context.write(m_key, m_val);
		}
	}
}
