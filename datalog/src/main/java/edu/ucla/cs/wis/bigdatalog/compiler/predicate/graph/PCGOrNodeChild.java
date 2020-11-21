package edu.ucla.cs.wis.bigdatalog.compiler.predicate.graph;

import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;

public interface PCGOrNodeChild extends PCGNodeChild {
	
	CompilerTypeBase copy();
	
	CompilerTypeBase copy(CompilerVariableList variableList);
}
