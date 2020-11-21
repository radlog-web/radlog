package edu.ucla.cs.wis.bigdatalog.api;

import edu.ucla.cs.wis.bigdatalog.system.LogLevel;

public class SystemOptions {	
	private int		logLevel;
	private int		timer;

	public SystemOptions(int timer, int logLevel) {
		this.timer			= timer;
		this.logLevel		= logLevel;
	}

	public int getLogLevel() { return this.logLevel; }
	
	public void setLogLevel(LogLevel logLevel) { this.logLevel |= logLevel.getLogLevelId(); }
	
	public void unsetLogLevel(LogLevel logLevel) {
		// don't toggle, only turn off it is is on
		if ((this.logLevel & logLevel.getLogLevelId()) == logLevel.getLogLevelId())
			this.logLevel ^= logLevel.getLogLevelId();
	}

	public void setTimer(TimerOption timerOption) { this.timer |= timerOption.getTimerOptionId(); }

	public void setTimerOff() { this.timer = TimerOption.NONE.getTimerOptionId(); }

	public boolean isTimerOn() { return (this.timer > 0); }
	
	public boolean isTimerOn(TimerOption timerOption) { 
		int timerOptionId = timerOption.getTimerOptionId(); 
		return (this.timer & timerOptionId) == timerOptionId; 
	}
	
	public String toStringLogLevelSetting() {
		StringBuilder output = new StringBuilder();
		int count = 0;
		for (LogLevel ll : LogLevel.values()) {
			if (count > 0)
				output.append("\n");
			output.append(ll.name());
			output.append(" = ");
			if ((ll.getLogLevelId() & this.logLevel) == ll.getLogLevelId())
				output.append(CommandOption.ON.getText().toUpperCase());
			else
				output.append(CommandOption.OFF.getText().toUpperCase());
			
			count++;
		}
		return output.toString();
	}
	
	public String toStringTimerSetting() {
		return "timer = " + this.timer;
	}

	public String toString() {
		StringBuilder output = new StringBuilder();
		output.append(this.toStringLogLevelSetting());
		output.append("\n");
		output.append(this.toStringTimerSetting());
		return output.toString();
	}
}
