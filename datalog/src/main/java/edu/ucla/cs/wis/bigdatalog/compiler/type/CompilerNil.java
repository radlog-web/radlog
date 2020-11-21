package edu.ucla.cs.wis.bigdatalog.compiler.type;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;

public class CompilerNil extends CompilerTypeBase {
	private static final long serialVersionUID = 1L;

	public CompilerNil() {
		super(CompilerType.COMPILER_NIL);
	}

	@Override
	public CompilerTypeBase copy() { return new CompilerNil(); }

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) { return new CompilerNil(); }

	@Override
	public boolean equals(CompilerTypeBase other) { return false; }

	@Override
	public String toString() { return CompilerTypeBase.NIL; }
}
