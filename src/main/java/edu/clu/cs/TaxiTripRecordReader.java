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
		m_lineValue.setRateCode(TripDataFormatHelper.s_rateCodeFactor
				.convertToFloat(fields[3]));
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

		m_lineValue.setPassengerCount(TripDataFormatHelper.s_passengerCount
				.convertToFloat(fields[7]));

		m_lineValue.setTripTimeInSecs(TripDataFormatHelper.s_tripTimeInSecs
				.convertToFloat(fields[8]));

		m_lineValue.setTripDistance(TripDataFormatHelper.s_tripDistance
				.convertToFloat(fields[9]));

		m_lineValue.setPickupLongitude(TripDataFormatHelper.s_pickupLongitude
				.convertToFloat(fields[10]));

		m_lineValue.setPickupLatitude(TripDataFormatHelper.s_pickupLatitude
				.convertToFloat(fields[11]));

		m_lineValue.setDropoffLongitude(TripDataFormatHelper.s_dropoffLongitude
				.convertToFloat(fields[12]));

		m_lineValue.setDropoffLatitude(TripDataFormatHelper.s_dropoffLatitude
				.convertToFloat(fields[13]));

		m_lineValue.setPaymentType(TripDataFormatHelper.s_paymentTypeFactor
				.convertToFloat(fields[14]));

		m_lineValue.setFareAmount(TripDataFormatHelper.s_fareAmount
				.convertToFloat(fields[15]));

		m_lineValue.setSurcharge(TripDataFormatHelper.s_surcharge
				.convertToFloat(fields[16]));

		m_lineValue.setMtaTax(TripDataFormatHelper.s_mtaTax
				.convertToFloat(fields[17]));

		m_lineValue.setTipAmount(TripDataFormatHelper.s_tipAmount
				.convertToFloat(fields[18]));

		m_lineValue.setTollsAmount(TripDataFormatHelper.s_tollsAmount
				.convertToFloat(fields[19]));

		m_lineValue.setTotalAmount(TripDataFormatHelper.s_totalAmount
				.convertToFloat(fields[20]));

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
