package edu.clu.cs;

import java.util.HashMap;
import java.util.TreeSet;

/**
 * 
 * @author cloudera
 *
 */

public class CmdArgumentParser {
	/**
	 * Option types
	 *   - Optional The option can be omitted
	 *   - Required The option can not be omitted
	 */
	public enum OptionType {
		Optional, 
		Required
	}

	/**
	 * 
	 */
	private HashMap<String, String> m_options = null;
	
	/**
	 * 
	 */
	private HashMap<String, OptionType> m_schema = null;
	
	/**
	 * Constructor
	 * @param schema The options and whether they are requried
	 * @param defaultValues Default values
	 */
	public CmdArgumentParser(HashMap<String, OptionType> schema,
			HashMap<String, String> defaultValues) {
		if (null == schema) {
			throw new IllegalArgumentException("Schema should not be null.");
		}
		m_schema = schema;
		m_options = defaultValues;
	}

	/**
	 * Parse the comamnd line arguemnt list and stores the data
	 * @param args The command line argument list
	 * @throws CmdArgumentsException
	 */
	public void parse(String[] args) throws CmdArgumentsException {
		if (null == m_options) {
			m_options = new HashMap<String, String>();
		}

		TreeSet<String> reqOptions = new TreeSet<String>();
		for (String key : m_schema.keySet()) {
			OptionType tp = m_schema.get(key);
			if (OptionType.Required == tp ) {
				reqOptions.add(key);
			}
		}

		boolean parsingOption = false;
		String lastOption = null;

		for (int i = 0; i < args.length; ++i) {
			if (parsingOption) { // We are parsing an option and is expecting an
									// argument
				m_options.put(lastOption, args[i]);
				lastOption = null;
				parsingOption = false;
			} else { // We are currently not paring an option
				if (args[i].charAt(0) == '-') { // We meet an option
					assert (m_options.containsKey(args[i]));
					OptionType tp = m_schema.get(args[i]);
					if (reqOptions.contains(args[i])) {
						reqOptions.remove(args[i]);
					}
					lastOption = args[i];
					parsingOption = true;
				} else { // In this implementation, all arguments must be
							// options
					assert (false);
				}
			}
		}
		if (!reqOptions.isEmpty()) {
			throw new CmdArgumentsException("Required option(s) not specified.");
		}
		if (parsingOption) {
			throw new CmdArgumentsException("Argument list ended while reading options.");
		}
	}

	/**
	 * Retrieve parsed options
	 * @param option Option name
	 * @return The argument of the option
	 */
	public String get(String option) {
		if (null==m_options) {
			throw new IllegalStateException("Accessing option value before parsing argumetns.");
		}
		return m_options.get(option);
	}
	
//	static public void main(String[] args) throws CmdArgumentsException {
//		HashMap<String, CmdArgumentParser.OptionType> schema = new HashMap<String, CmdArgumentParser.OptionType>();
//		schema.put("-i", CmdArgumentParser.OptionType.Required);
//		schema.put("-o", CmdArgumentParser.OptionType.Required);
//		schema.put("-f", CmdArgumentParser.OptionType.Optional);
//		CmdArgumentParser parser = new CmdArgumentParser(schema, null);
//		parser.parse(args);
//		System.out.println(parser.get("-i"));
//		System.out.println(parser.get("-o"));
//		System.out.println(parser.get("-f"));
//	}
}

