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
    	Numerical,
    	Categorical,
    }
    
    private static final String[] s_VenderIdLevels = {"CMT","VTS"};
    private static final String[] s_PaymentTypeLevels = {"CRD", "CSH", "DIS", "NOC", "UNK"};
    private static final String[] s_StoreFwdFlagLevels = { "Y", "N" };
    
    public static Factor s_venderIdFactor = new Factor(s_VenderIdLevels);
    public static Factor s_paymentTypeFactor = new Factor(s_PaymentTypeLevels);
    public static Factor s_storeFwdFlagFactor = new Factor(s_StoreFwdFlagLevels);
    
	static public final String[]  s_colNames = {
		"medallion",
		"hack_license",
		"vendor_id",
		"rate_code",
		"store_and_fwd_flag",
		"pickup_datetime",
		"dropoff_datetime",
		"passenger_count",
		"trip_time_in_secs",
		"trip_distance",
		"pickup_longitude",
		"pickup_latitude",
		"dropoff_longitude",
		"dropoff_latitude",
		"payment_type",
		"fare_amount",
		"surcharge",
		"mta_tax",
		"tip_amount",
		"tolls_amount",
		"total_amount"
	};
	
	static public final VariableType[] s_colTypes = {
		VariableType.Categorical,
		VariableType.Categorical,
		VariableType.Categorical,
		VariableType.Numerical,
		VariableType.Categorical,
		VariableType.Numerical,
		VariableType.Numerical,
		VariableType.Numerical,
		VariableType.Numerical,
		VariableType.Numerical,
		VariableType.Numerical,
		VariableType.Numerical,
		VariableType.Numerical,
		VariableType.Numerical,
		VariableType.Categorical,
		VariableType.Numerical,
		VariableType.Numerical,
		VariableType.Numerical,
		VariableType.Numerical,
		VariableType.Numerical,
		VariableType.Numerical,
	};
	
	/**
	 * Translate a column index or a name into a valid column name.
	 * 
	 * @param arg  a column name or column index in string
	 * @return a valid column name
	 */
	public static String getColName(String arg) {
		try {
			int col_num = Integer.parseInt(arg);
			if (col_num >=0 && col_num < s_colNames.length) {
				return s_colNames[col_num];
			} else {
				return s_colNames[0];
			}
		} catch (NumberFormatException e) {
			if (contains(arg)) {
				return arg;
			} else {
				return s_colNames[0];
			}
		}
	}

	/**
	 * Translate a column index or a name into a valid column index.
	 * @param arg  a column name or column index in string
	 * @return a valid column index
	 */
	public static int getColIndex(String arg) {
		
		try {
			int col_num = Integer.parseInt(arg);
			if (col_num >=0 && col_num < s_colNames.length) {
				return col_num;
			} else {
				return 0;
			}
		} catch (NumberFormatException e) {
			if (contains(arg)) {
				return indexOf(arg);
			} else {
				return 0;
			}
		}
	}
	
	/**
	 * Detect whether the argument string is a field name
	 * @param arg a string that may be a field name
	 * @return whether the argument string is a field name
	 */
	private static boolean contains(String arg) {
		for (int i=0; i<s_colNames.length; ++i) {
			if (s_colNames[i].equals(arg)) {
				return true;
			}
		}
		return false;
	}
	
	/**
	 * Find the index given a field name
	 * @param arg  string that may be a field name
	 * @return The corresponding index (default to 0)
	 */
	private static int indexOf(String arg) {
		for (int i=0; i<s_colNames.length; ++i) {
			if (s_colNames[i].equals(arg)) {
				return i;
			}
		}
		return -1;
	}
	
//	static public void main(String[] args) {
//		System.out.println(getColNum("trip_distance"));
//	}
}
