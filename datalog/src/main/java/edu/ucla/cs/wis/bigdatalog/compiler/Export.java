package edu.ucla.cs.wis.bigdatalog.compiler;

import com.google.gson.Gson;

import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerTypeBase;
import edu.ucla.cs.wis.bigdatalog.compiler.type.CompilerType;
import edu.ucla.cs.wis.bigdatalog.compiler.variable.CompilerVariableList;

public class Export extends CompilerTypeBase {
	protected QueryForm queryForm;
	
	public Export(QueryForm queryForm) {
		super(CompilerType.EXPORT);
		this.queryForm = queryForm;
	}

	public QueryForm getQueryForm() { return this.queryForm; }

	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("export definition ");
		retval.append(this.queryForm.toString());
		return retval.toString();
	}
	
	public String toJson() {
		StringBuilder retval = new StringBuilder();
		retval.append("{\"predicate\":\"");
		retval.append(this.queryForm.getPredicateName());
		retval.append("\", \"arguments\":[");
		Gson gson = new Gson();
		for (int i = 0; i < this.queryForm.getArguments().size(); i++) {
			if (i > 0)
				retval.append(",");
			retval.append(gson.toJson(this.queryForm.getArgument(i)));
		}
		retval.append("]}");
		return retval.toString();
	}

	public Export copy() {
		return new Export(this.queryForm.copy());
	}

	@Override
	public CompilerTypeBase copy(CompilerVariableList variableList) {
		return copy();
	}

	@Override
	public boolean equals(CompilerTypeBase other) {
		if (other == null)
			return false;
		
		if (!(other instanceof Export))
			return false;
		
		Export otherExport = (Export)other;
		if (this.getQueryForm() != null && otherExport.getQueryForm() == null)
			return false;
		
		return this.getQueryForm().equals(otherExport.getQueryForm());
	}
}