package edu.clu.cs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;

public class NumericalSummaryMapper extends
		Mapper<LongWritable, TripDataTuple, Text, FloatWritable> {
	private FloatWritable m_val = new FloatWritable();
	private Text m_colKey = new Text();

	@Override
	public void map(LongWritable key, TripDataTuple value, Context context)
			throws IOException, InterruptedException {

		m_colKey.set("rate_code");
		m_val.set(value.getRateCode());
		context.write(m_colKey, m_val);

		m_colKey.set("passenger_count");
		m_val.set(value.getPassengerCount());
		context.write(m_colKey, m_val);

		m_colKey.set("trip_time_in_secs");
		m_val.set(value.getTripTimeInSecs());
		context.write(m_colKey, m_val);

		m_colKey.set("trip_distance");
		m_val.set(value.getTripDistance());
		context.write(m_colKey, m_val);

		m_colKey.set("pickup_longitude");
		m_val.set(value.getPickupLongitude());
		context.write(m_colKey, m_val);

		m_colKey.set("pickup_latitude");
		m_val.set(value.getPickupLatitude());
		context.write(m_colKey, m_val);

		m_colKey.set("dropoff_longitude");
		m_val.set(value.getDropoffLongitude());
		context.write(m_colKey, m_val);

		m_colKey.set("drop_off_latitude");
		m_val.set(value.getDropoffLatitude());
		context.write(m_colKey, m_val);

		m_colKey.set("fare_amount");
		m_val.set(value.getFareAmount());
		context.write(m_colKey, m_val);

		m_colKey.set("surcharge");
		m_val.set(value.getSurcharge());
		context.write(m_colKey, m_val);

		m_colKey.set("mta_tax");
		m_val.set(value.getMtaTax());
		context.write(m_colKey, m_val);

		m_colKey.set("tip_amount");
		m_val.set(value.getTipAmount());
		context.write(m_colKey, m_val);

		m_colKey.set("tolls_amount");
		m_val.set(value.getTollsAmount());
		context.write(m_colKey, m_val);

		m_colKey.set("total_amount");
		m_val.set(value.getTotalAmount());
		context.write(m_colKey, m_val);

	}
}
