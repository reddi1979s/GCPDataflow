package com.apache.dataflow.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.options.ValueProvider;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.Watch;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code TextToPubsub} pipeline publishes records to Cloud Pub/Sub from a
 * set of files. The pipeline reads each file row-by-row and publishes each
 * record as a string message. At the moment, publishing messages with
 * attributes is unsupported.
 *
 * <p>
 * Example Usage:
 *
 * <pre>
 * {@code mvn compile exec:java \
    -Dexec.mainClass=com.google.cloud.teleport.templates.TextToPubsub \
    -Dexec.args=" \
    --project=${PROJECT_ID} \
    --stagingLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/staging \
    --tempLocation=gs://${PROJECT_ID}/dataflow/pipelines/${PIPELINE_FOLDER}/temp \
    --runner=DataflowRunner \
    --inputFilePattern=gs://path/to/demo_file.csv \
    --outputTopic=projects/${PROJECT_ID}/topics/${TOPIC_NAME}"
 * }
 * </pre>
 *
 */
public class TextToPubsub {

	private static final Duration DEFAULT_POLL_INTERVAL = Duration.standardSeconds(10);

	private static final Logger LOG = LoggerFactory.getLogger(TextToPubsub.class);

	/**
	 * The custom options supported by the pipeline. Inherits standard configuration
	 * options.
	 */
	public interface Options extends PipelineOptions {
		@Description("The file pattern to read records from (e.g. gs://bucket/file-*.csv)")
		@Required
		ValueProvider<String> getInputFilePattern();

		void setInputFilePattern(ValueProvider<String> value);

		@Description("The name of the topic which data should be published to. "
				+ "The name should be in the format of projects/<project-id>/topics/<topic-name>.")
		@Required
		ValueProvider<String> getOutputTopic();

		void setOutputTopic(ValueProvider<String> value);
	}

	/**
	 * Main entry-point for the pipeline. Reads in the command-line arguments,
	 * parses them, and executes the pipeline.
	 *
	 * @param args Arguments passed in from the command-line.
	 */
	public static void main(String[] args) {

		// Parse the user options passed from the command-line
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);

		run(options);
	}

	/**
	 * Executes the pipeline with the provided execution parameters.
	 *
	 * @param options The execution parameters.
	 */
	public static PipelineResult run(Options options) {
		// Create the pipeline.
		Pipeline pipeline = Pipeline.create(options);

		/*
		 * Steps: 1) Read from the text source. 2) Write each text record to Pub/Sub
		 */
		LOG.info("Running pipeline");
		LOG.info("Input : " + options.getInputFilePattern());
		LOG.info("Output : " + options.getOutputTopic());

		pipeline.apply("Read Text Data", TextIO.read().from(options.getInputFilePattern())
				.watchForNewFiles(DEFAULT_POLL_INTERVAL, Watch.Growth.never()))
				.apply("Write logs", ParDo.of(new DoFn<String, String>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) throws Exception {
						LOG.info(c.element());
						c.output(c.element());
					}
				})).apply("Write logs", ParDo.of(new DoFn<String, String>() {
					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					@ProcessElement
					public void processElement(ProcessContext c) throws Exception {
						getShortString(c.element(), 0);
					}
				})).apply("Write to PubSub", PubsubIO.writeStrings().to(options.getOutputTopic()));

		return pipeline.run();
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