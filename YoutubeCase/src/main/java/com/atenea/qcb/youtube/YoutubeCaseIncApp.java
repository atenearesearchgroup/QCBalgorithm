package com.atenea.qcb.youtube;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import com.atenea.qcb.youtube.incremental.IncrementalQCBThread;
import com.atenea.qcb.youtube.incremental.MainThread;


public class YoutubeCaseIncApp {
	private final static Logger LOGGER = Logger.getLogger(YoutubeCaseIncApp.class.getName());

	static int threads = 8;
	// This executor distributes the work between the cores
	static ExecutorService executor = Executors.newFixedThreadPool(threads);

	@SuppressWarnings("deprecation")
	public static <E> void main(String[] args) throws Exception {
		FileHandler fh;

		try (InputStream input = new FileInputStream("src/main/resources/config.properties")) {

			Properties prop = new Properties();

			// load a properties file
			prop.load(input);
			String fileWeight = prop.getProperty("nameWeights");
			String records = prop.getProperty("records");
			String recordsQuery = prop.getProperty("recordsQuery");
			String query = prop.getProperty("query");

			// handler
			fh = new FileHandler("MyYoutubeLogFileIncremental" + fileWeight + "-" + query + "-" + records + "-" + recordsQuery + ".log",
					true);
			LOGGER.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();
			fh.setFormatter(formatter);

			MainThread mainThread = new MainThread(fh);
			Thread thread1 = new Thread(mainThread);

			// First let's start the incremental, which will be running always
			IncrementalQCBThread incrementalAlgThread = new IncrementalQCBThread(mainThread);
			// incrementalAlgThread.run();
			Thread thread2 = new Thread(incrementalAlgThread);

			Long init = System.currentTimeMillis();
			LOGGER.info(init + " timestamp init updating");
			thread1.start();
			thread2.start();
			thread1.join();

			Long end = System.currentTimeMillis();
			LOGGER.info(end + " timestamp end updating");
			LOGGER.info(end - init + " milliseconds to finish the updating");

			thread2.stop();

		}
	}
}
