package edu.ucla.cs.wis.bigdatalog.compiler.aggregate;

public enum FSCountNodeType {
	//SINGLE("single"),
	DOUBLE("double"),
	DOUBLEDELTA("doubledelta"),
	//BPLUSDOUBLEDELTA("bplusdoubledelta"),
	KEYVALUESTORE("keyvaluestore");
	
	public String name;
	private FSCountNodeType(String name) {
		this.name = name;
	}
	
	public String getName() { return this.name; }
	
	public String toString() { return this.name; }
	
	public static FSCountNodeType getNodeType(String name) {
		for (FSCountNodeType nodeType : FSCountNodeType.values())
			if (nodeType.name.equals(name))
				return nodeType;
				
		return null;
	}
}