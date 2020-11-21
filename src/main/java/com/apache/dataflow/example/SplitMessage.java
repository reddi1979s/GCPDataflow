package com.apache.dataflow.example;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class SplitMessage extends PTransform<PCollection<String>, PCollection<String>> {

	/**
	 * This class is used to split the string based on user defined delimeter.
	 */
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) {
		
	}

	private void myDelimited(String line) {

		String countPosition = line.substring(2, 6);
		int msglenght = Integer.parseInt(countPosition);
		int segmentLenght = 106;

		// messageArr.add(line.substring(length1, (msglenght * segmentLenght) - 41));
		int maxLengthOfPart = (msglenght * segmentLenght) - 41;
		String result = getShortString(line, maxLengthOfPart);
		System.out.println("*********************************************");
		System.out.println(result);
		System.out.println("*********************************************");
	}

	/**
	 * @param fullLongString  Long String value
	 * @param maxLengthOfPart Maximum length of the smaller String
	 * @return String result as a short String
	 */
	public static String getShortString(String fullLongString, int maxLengthOfPart) {
		String countPosition = fullLongString.substring(2, 6);
		int msglenght = Integer.parseInt(countPosition);
		int segmentLenght = 106;
		maxLengthOfPart = (msglenght * segmentLenght) - 41;
		if ((fullLongString == null) || (fullLongString.trim().equals("")) || (maxLengthOfPart <= 0)
				|| (fullLongString.length() <= maxLengthOfPart)) {
			return fullLongString;
		} else {
			String firstPart = fullLongString.substring(0, maxLengthOfPart);
			return firstPart + "\n" + getShortString(fullLongString.substring(maxLengthOfPart, fullLongString.length()),
					maxLengthOfPart);
		}

	}

	@Override
	public PCollection<String> expand(PCollection<String> line) {
		// Convert line of text into individual lines
		PCollection<String> lines = line.apply(ParDo.of(new SplitWordsFn()));

		return lines;
	}
}