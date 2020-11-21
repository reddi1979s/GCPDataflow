package com.apache.dataflow.example;

import org.apache.beam.sdk.transforms.DoFn;

public class SplitWordsFn extends DoFn<String, String> {

	/**
	 * This class contains processElement which calls the getShortString method to
	 * split stream line string based on segments.
	 */
	private static final long serialVersionUID = 1L;

	@ProcessElement
	public void processElement(ProcessContext c) {

		getShortString(c.element(), 0);
	}

	public static String getShortString(String fullLongString, int maxLengthOfPart) {

		try {
			String countPosition = fullLongString.substring(2, 6);
			int msglenght = Integer.parseInt(countPosition);
			int segmentLenght = 106;
			maxLengthOfPart = (msglenght * segmentLenght) - 41;
			if ((fullLongString == null) || (fullLongString.trim().equals("")) || (maxLengthOfPart <= 0)
					|| (fullLongString.length() <= maxLengthOfPart)) {
				return fullLongString;
			} else {
				String firstPart = fullLongString.substring(0, maxLengthOfPart);
				return firstPart + "\n" + getShortString(
						fullLongString.substring(maxLengthOfPart, fullLongString.length()), maxLengthOfPart);
			}
		} catch (Exception e) {
			
		}
		return fullLongString;
	}
}
