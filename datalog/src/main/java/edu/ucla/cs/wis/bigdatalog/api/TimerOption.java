package edu.ucla.cs.wis.bigdatalog.api;

public enum TimerOption {
	NONE(0),
	QUERY_SETUP(1), 
	QUERY(2), 
	QUERY_CLEANUP(3);
	
	private int timerOptionId;
	
	private TimerOption(int timerOptionId) { this.timerOptionId = timerOptionId; }

	public int getTimerOptionId() { return this.timerOptionId; }
	
	public static TimerOption getTimerOption(int timerOptionId) {
		for (TimerOption timerOption : TimerOption.values()) {
			if (timerOption.timerOptionId == timerOptionId)
				return timerOption;
		}
		return null;
	}
	
}
