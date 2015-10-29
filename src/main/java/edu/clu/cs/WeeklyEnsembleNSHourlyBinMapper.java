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

public class WeeklyEnsembleNSHourlyBinMapper extends
		Mapper<LongWritable, TripDataTuple, Text, FloatWritable> {
	private Text m_key = new Text();
	private FloatWritable m_val = new FloatWritable();

	@Override
	public void map(LongWritable key, TripDataTuple value, Context context)
			throws IOException, InterruptedException {

		String kstr = "PickupCount - "
				+ Integer.toString(value.getPickupDatetime().get(Calendar.DAY_OF_WEEK))
				+ " - "
				+ Integer.toString(value.getPickupDatetime().get(
						Calendar.HOUR_OF_DAY));
		m_key.set(kstr);
		m_val.set(1);
		context.write(m_key, m_val);

		kstr = "DropoffCount - "
				+ Integer.toString(value.getPickupDatetime().get(Calendar.DAY_OF_WEEK))
				+ " - "
				+ Integer.toString(value.getDropoffDatetime().get(
						Calendar.HOUR_OF_DAY));
		m_key.set(kstr);
		m_val.set(1);
		context.write(m_key, m_val);

		kstr = "TotalAmount - "
				+ Integer.toString(value.getPickupDatetime().get(Calendar.DAY_OF_WEEK))
				+ " - "
				+ Integer.toString(value.getDropoffDatetime().get(
						Calendar.HOUR_OF_DAY));
		m_key.set(kstr);
		m_val.set(value.getTotalAmount());
		context.write(m_key, m_val);

		kstr = "TipAmount - "
				+ Integer.toString(value.getPickupDatetime().get(Calendar.DAY_OF_WEEK))
				+ " - "
				+ Integer.toString(value.getDropoffDatetime().get(
						Calendar.HOUR_OF_DAY));
		m_key.set(kstr);
		m_val.set(value.getTipAmount());
		context.write(m_key, m_val);
	}

}
