package edu.ucla.cs.wis.bigdatalog.interpreter;

public enum EvaluationType {
	Naive("naive", 1),
	SemiNaive("sn", 2),
	XY("xy",3),
	MonotonicSemiNaive("msn",4), // uses two relations - one recursive, one aggregate
	EagerMonotonic("emsn",9), // no recursive relation, uses only aggregate relation
	SSC("ssc", 10),
	JavaImp("javaimp", 11);

	private String nickname;
	private int evaluationTypeId;	
	
	public String getNickname() { return this.nickname; }
	
	public int getEvaluationTypeId() { return this.evaluationTypeId; }
	
	private EvaluationType(String nickname, int evaluationTypeId) {
		this.nickname = nickname;
		this.evaluationTypeId = evaluationTypeId;
	}
	
	public static EvaluationType getEvaluationType(String name) {
		for (EvaluationType evaluationType : EvaluationType.values())
			if (evaluationType.getNickname().equals(name))
				return evaluationType;
			
		return null;
	}
}
