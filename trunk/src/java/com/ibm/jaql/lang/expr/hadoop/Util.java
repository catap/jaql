/*
 * Copyright (C) IBM Corp. 2008.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.ibm.jaql.lang.expr.hadoop;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;

import org.apache.commons.lang.UnhandledException;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.log4j.Logger;

import com.ibm.jaql.json.type.JsonString;
import com.ibm.jaql.json.type.JsonUtil;

public class Util {
	
	public final static Logger STATUS_LOG = Logger.getLogger("com.ibm.jaql.status.MapReduce");
	public final static Logger        LOG = Logger.getLogger(Util.class.getName());
	public final static String FETCH_SYSLOG_PROP = "jaql.mapred.fetchsyslog";

	private static String mapReduceStatusFmt = "{class: %1$s, msg: %2$s}";
	private static String mapReduceInfoFmt = "{class: %1$s, msg: %2$s, id: %3$s, name: %4$s, url: %5$s}";
	private static String STOP;
	private static String START;
	private static String INFO;
	
	static {
		try {
			STOP = JsonUtil.printToString(new JsonString("STOP"));
			START = JsonUtil.printToString(new JsonString("START"));
			INFO = JsonUtil.printToString(new JsonString("INFO"));
		} catch(Exception e) {
			throw new UnhandledException(e);
		}
	}
	
	public static void mrStatusStart(String submitClassName) {
		STATUS_LOG.info(String.format(mapReduceStatusFmt, submitClassName, START));
	}
	
	public static void mrStatusStop(String submitClassName) {
		STATUS_LOG.info(String.format(mapReduceStatusFmt, submitClassName, STOP));
	}
	
	public static void mrStatusInfo(String submitClassName, String jobId, String jobName, String trackingUrl) {
		STATUS_LOG.info(String.format(mapReduceInfoFmt, submitClassName, INFO, jobId, jobName, trackingUrl));
	}
	
	public static void submitJob(JsonString submitClassName, JobConf conf) throws Exception {
		JobClient jc = new JobClient(conf);
		RunningJob rj = jc.submitJob(conf);
		String sc = JsonUtil.printToString(submitClassName);
		
		// log to status that a MR job is starting
		mrStatusStart(sc);
		
		// log to status vital MR job information
		mrStatusInfo(sc, JsonUtil.printToString(new JsonString(rj.getID().toString())), 
				         JsonUtil.printToString(new JsonString(rj.getJobName())), 
				         JsonUtil.printToString(new JsonString(rj.getTrackingURL())));
		//STATUS_LOG.info("MAP-REDUCE INFO: " + rj.getID() + "," + rj.getJobName() + "," + rj.getTrackingURL());
		
		boolean failed = false;
		try {
			if (!jc.monitorAndPrintJob(conf, rj)) {
				LOG.error(new IOException("Job failed!"));
				failed = true;
				//throw new IOException("Job failed!");
			}
		} catch (InterruptedException ie) {
			Thread.currentThread().interrupt();
		}

		try {
			if( System.getProperty(FETCH_SYSLOG_PROP, "false").toLowerCase().equals("true") ) {
				if( rj.isSuccessful() ) {
					logAllTaskSyslogs(rj, true);
				} else {
					logAllTaskSyslogs(rj, false);
				}
			}
		} catch(Throwable t) {
			// log it, but do not stop the world for this
			LOG.error(t);
		}
		
		// log to status that a MR job is stopping
		mrStatusStop(sc);
		
		// if the job failed, then throw an exception
		if(failed) {
			throw new IOException("Job failed!");
		}
	}	
    
	public static void logAllTaskSyslogs(RunningJob rj, boolean onlySuccessful) throws Exception {
		String fetch = System.getProperty("jaql.cluster.fetchlogs");
		if( fetch != null && fetch.equals("false"))
			return;
		TaskCompletionEvent[] events = rj.getTaskCompletionEvents(0);
		for(TaskCompletionEvent event : events) {
			if( onlySuccessful && (event.getTaskStatus() == TaskCompletionEvent.Status.SUCCEEDED) ) {
				// print the syslog into the main log
				STATUS_LOG.info(event.toString());
				logTaskSyslogs(event.getTaskAttemptId(), event.getTaskTrackerHttp());
			} else {
				STATUS_LOG.info(event.toString());
				logTaskSyslogs(event.getTaskAttemptId(), event.getTaskTrackerHttp());
			}
		}
	}
	
    /**
     * Taken from Hadoop's JobClient
     * 
     * @param taskId
     * @param baseUrl
     * @return
     */
    public static String getTaskLogURL(TaskAttemptID taskId, String baseUrl) {
		return (baseUrl + "/tasklog?plaintext=true&taskid=" + taskId); 
	}
    
	/**
	 * Modified from Hadoop's JobClient
	 * 
	 * @param taskId
	 * @param baseUrl
	 * @throws IOException
	 */
	public static void logTaskSyslogs(TaskAttemptID taskId, String baseUrl)
	throws IOException {
		// The tasktracker for a 'failed/killed' job might not be around...
		if (baseUrl != null) {
			// Construct the url for the tasklogs
			String taskLogUrl = getTaskLogURL(taskId, baseUrl);

			// copy task logs into jaql log
			logTaskLog(taskId, new URL(taskLogUrl+"&filter=syslog"));
		}
	}

	/**
	 * Modified from Hadoop's JobClient
	 * 
	 * @param taskId
	 * @param taskLogUrl
	 */
	private static void logTaskLog(TaskAttemptID taskId, URL taskLogUrl) {
		try {
			URLConnection connection = taskLogUrl.openConnection();
			BufferedReader input = 
				new BufferedReader(new InputStreamReader(connection.getInputStream()));
			try {
				String logData = null;
				while ((logData = input.readLine()) != null) {
					if (logData.length() > 0) {
						LOG.info("["+taskId+"]" + logData);
					}
				}
			} finally {
				input.close();
			}
		}catch(IOException ioe){
			LOG.warn("Error reading task output" + ioe.getMessage()); 
		}
	}
	  
}