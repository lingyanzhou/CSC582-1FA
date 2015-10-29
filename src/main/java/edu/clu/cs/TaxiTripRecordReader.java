package edu.clu.cs;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class TaxiTripRecordReader extends
		RecordReader<LongWritable, TripDataTuple> {

	private LineRecordReader m_lineReader = new LineRecordReader();
	private TripDataTuple m_lineValue = new TripDataTuple();
	private SimpleDateFormat m_formatter = new SimpleDateFormat(
			"yyyy-MM-dd HH:mm:ss");

	@Override
	public void close() throws IOException {
		m_lineReader.close();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException,
			InterruptedException {
		return m_lineReader.getCurrentKey();
	}

	@Override
	public TripDataTuple getCurrentValue() throws IOException,
			InterruptedException {
		m_lineValue.clear();
		String line = m_lineReader.getCurrentValue().toString();
		String[] fields = line.split(",");
		m_lineValue.setMedallion(fields[0]);
		m_lineValue.setHackLicense(fields[1]);
		m_lineValue.setVendorId(TripDataFormatHelper.s_venderIdFactor
				.convertToFloat(fields[2]));
		m_lineValue.setRateCode(Float.parseFloat(fields[3]));
		m_lineValue
				.setStoreAndFwdFlag(TripDataFormatHelper.s_storeFwdFlagFactor
						.convertToFloat(fields[4]));
		try {
			m_lineValue.setPickupDatetime(m_formatter.parse(fields[5])
					.getTime());

		} catch (ParseException e) {
			throw new IOException(e);
		}
		try {
			m_lineValue.setDropoffDatetime(m_formatter.parse(fields[6])
					.getTime());

		} catch (ParseException e) {
			throw new IOException(e);
		}
		try {
			m_lineValue.setPassengerCount(Float.parseFloat(fields[7]));
		} catch (NumberFormatException e) {
		}
		try {
			m_lineValue.setTripTimeInSecs(Float.parseFloat(fields[8]));
		} catch (NumberFormatException e) {
		}
		try {
			m_lineValue.setTripDistance(Float.parseFloat(fields[9]));
		} catch (NumberFormatException e) {
		}
		try {
			m_lineValue.setPickupLongitude(Float.parseFloat(fields[10]));
		} catch (NumberFormatException e) {
		}
		try {
			m_lineValue.setPickupLatitude(Float.parseFloat(fields[11]));
		} catch (NumberFormatException e) {
		}
		try {
			m_lineValue.setDropoffLongitude(Float.parseFloat(fields[12]));
		} catch (NumberFormatException e) {
		}
		try {
			m_lineValue.setDropoffLatitude(Float.parseFloat(fields[13]));
		} catch (NumberFormatException e) {
		}
		m_lineValue.setPaymentType(TripDataFormatHelper.s_paymentTypeFactor
				.convertToFloat(fields[14]));
		try {
			m_lineValue.setFareAmount(Float.parseFloat(fields[15]));
		} catch (NumberFormatException e) {
		}
		try {
			m_lineValue.setSurcharge(Float.parseFloat(fields[16]));
		} catch (NumberFormatException e) {
		}
		try {
			m_lineValue.setMtaTax(Float.parseFloat(fields[17]));
		} catch (NumberFormatException e) {
		}
		try {
			m_lineValue.setTipAmount(Float.parseFloat(fields[18]));
		} catch (NumberFormatException e) {
		}
		try {
			m_lineValue.setTollsAmount(Float.parseFloat(fields[19]));
		} catch (NumberFormatException e) {
		}
		try {
			m_lineValue.setTotalAmount(Float.parseFloat(fields[20]));
		} catch (NumberFormatException e) {
		}

		return m_lineValue;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return m_lineReader.getProgress();
	}

	@Override
	public void initialize(InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		m_lineReader.initialize(arg0, arg1);

	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return m_lineReader.nextKeyValue();
	}

}
