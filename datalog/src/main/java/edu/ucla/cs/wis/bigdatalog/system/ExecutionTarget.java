package edu.ucla.cs.wis.bigdatalog.system;

public enum ExecutionTarget {
	DeALS_PIPELINED("pipelined"),
	OPERATORS("operators");
	
	private String nickname;
	
	public String getNickname() { return this.nickname; }
	
	private ExecutionTarget(String nickname) {
		this.nickname = nickname;
	}
	
	public static ExecutionTarget getExecutionTarget(String name) {
		for (ExecutionTarget executionTarget : ExecutionTarget.values())
			if (executionTarget.getNickname().equals(name))
				return executionTarget;
			
		return null;
	}
}
