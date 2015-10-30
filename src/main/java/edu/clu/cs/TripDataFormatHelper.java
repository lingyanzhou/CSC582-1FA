package edu.clu.cs;

/**
 * 
 * @author lingyan zhou
 * @version 0.01
 * @since Oct 21, 2015
 * 
 */
public class TripDataFormatHelper {
	public enum VariableType {
		Numerical, Categorical,
	}

	private static final String[] s_VenderIdLevels = { "CMT", "VTS" };
	private static final String[] s_PaymentTypeLevels = { "CRD", "CSH", "DIS",
			"NOC", "UNK" };
	private static final String[] s_StoreFwdFlagLevels = { "", "Y", "N" };
	private static final String[] s_rateCodeLevels = { "1", "2", "3", "4", "5",
			"6" };
	private static final String[] s_rateCodeLabels = { "Standard rate", "JFK",
			"Newark", "Nassau or Westchester", "Negotiated fare", "Group ride" };
	public static Factor s_venderIdFactor = new Factor(s_VenderIdLevels);
	public static Factor s_paymentTypeFactor = new Factor(s_PaymentTypeLevels);
	public static Factor s_storeFwdFlagFactor = new Factor(s_StoreFwdFlagLevels);
	public static Factor s_rateCodeFactor = new Factor(s_rateCodeLevels,
			s_rateCodeLabels);

	public static NumericalValue s_passengerCount = new NumericalValue(0, 8);
	public static NumericalValue s_tripTimeInSecs = new NumericalValue(0, 18000);
	public static NumericalValue s_tripDistance = new NumericalValue(0, 500);
	public static NumericalValue s_pickupLongitude = new NumericalValue(-74.05f, -73.70f);
	public static NumericalValue s_pickupLatitude = new NumericalValue(40.60f, 40.85f);
	public static NumericalValue s_dropoffLongitude = new NumericalValue(-74.05f, -73.70f);
	public static NumericalValue s_dropoffLatitude = new NumericalValue(40.60f, 40.85f);
	public static NumericalValue s_fareAmount = new NumericalValue(0, 500);
	public static NumericalValue s_surcharge = new NumericalValue(0, 1.5f);
	public static NumericalValue s_mtaTax = new NumericalValue(0, 1.0f);
	public static NumericalValue s_tipAmount = new NumericalValue(0, 200);
	public static NumericalValue s_tollsAmount = new NumericalValue(0, 200);
	public static NumericalValue s_totalAmount = new NumericalValue(0, 700);

}
