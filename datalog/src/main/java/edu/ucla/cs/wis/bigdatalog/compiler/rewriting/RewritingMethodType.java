package edu.ucla.cs.wis.bigdatalog.compiler.rewriting;

public enum RewritingMethodType {
	UNKNOWN("Unknown", false),
	TRIVIAL_PHASE1("Trivial Phase 1", true),
	TRIVIAL_PHASE2("Trivial Phase 2", true),
	LINEAR_MAGIC("Linear Magic", true),
	NON_LINEAR_MAGIC("Non-Linear Magic", true),
	GENERALIZED_MAGIC("Generalized Magic", true),
	SEMI_NAIVE("Semi-naive", false),
	NAIVE("Naive", false);
	
	private String name;
	private boolean isRewritingAllowed;

	private RewritingMethodType(String name, boolean isRewritingAllowed) {
		this.name = name;
		this.isRewritingAllowed = isRewritingAllowed; 
	}
	
	public String getName() { return this.name; }
	
	public boolean isRewritingAllowed() { return this.isRewritingAllowed;}
	
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append(this.name);
		if (this.isRewritingAllowed)
			retval.append(" (Rewriting allowed)");
		else 
			retval.append(" (No rewriting allowed)");		
		
		return retval.toString();		
	}
}
