package com.ibm.jaql.benchmark.util;

/* Should only be used carefully because states are not checked */
public class Timer {
	long startTime;
	long pauseStartTime;
	long totalPauseTime;
	long endTime;
	
	public Timer() {
		reset();
	}
	
	public void start() {
		startTime = System.nanoTime();
	}
	
	public void pause() {
		pauseStartTime = System.nanoTime();
	}
	
	public void resume() {
		totalPauseTime += System.nanoTime() - pauseStartTime;
	}
	
	public void stop() {
		endTime = System.nanoTime();
	}
	
	public long time() {
		return endTime - startTime - totalPauseTime;
	}
	
	public void reset() {
		startTime = 0;
		pauseStartTime = 0;
		totalPauseTime = 0;
		endTime = 0;
	}
}
