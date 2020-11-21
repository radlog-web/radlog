package edu.ucla.cs.wis.bigdatalog.system;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import edu.ucla.cs.wis.bigdatalog.compiler.*;
import edu.ucla.cs.wis.bigdatalog.compiler.Module;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicate;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicateIndex;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.BasePredicateKey;
import edu.ucla.cs.wis.bigdatalog.compiler.predicate.Predicate;
import edu.ucla.cs.wis.bigdatalog.exception.RelationManagerException;

public class ModuleManager implements Serializable {
	private static final long serialVersionUID = 1L;
	public static final String DEFAULT_MODULE_NAME = "default";
	
	private DeALSContext		deALSContext;
	private Module              activeModule;
	private Module				defaultModule;
	private List<Module>		modules;

	public ModuleManager(DeALSContext deALSContext) {
		this.deALSContext 	= deALSContext;
		this.modules 		= new ArrayList<>();

		this.defaultModule 	= new Module(this.deALSContext, this.deALSContext.getDatabase(), DEFAULT_MODULE_NAME);
		this.activeModule 	= this.defaultModule;
		this.modules.add(this.defaultModule);
	}

	public Module getActiveModule() { return this.activeModule; }
	
	public void setActiveModule(){
		this.setActiveModule(DEFAULT_MODULE_NAME);
	}

	public void setActiveModule(String moduleName) {
		for (Module module : this.modules) {
			if (module.getModuleName().equals(moduleName)) {
				this.setActiveModule(module);
				return;
			}
		}
		// if no module found by name, set to default module
		this.setActiveModule();
	}
	
	public void setActiveModule(Module module){
		if (module != null)
			this.activeModule = module;
		else
			this.activeModule = this.defaultModule;
	}

	public Module addModule(ModuleDeclaration moduleDeclaration) {
		for (Module module : this.modules)
			if (module.getModuleName().equals(moduleDeclaration.getModuleName()))
				return module.merge(moduleDeclaration);
		
		// did not find module - it is new
		Module module = new Module(this.deALSContext, this.deALSContext.getDatabase(), moduleDeclaration.getModuleName());
		module.merge(moduleDeclaration);
		this.modules.add(module);
		
		return module;
	}
	
	public boolean addExport(Export export) {
		return this.activeModule.addExport(export);
	}

	public boolean addRule(Rule rule) {
		BasePredicate schemaRelation;
		Predicate head = rule.getHead();
		schemaRelation = this.activeModule.getBasePredicate(head.getPredicateName());
		if (schemaRelation != null)
			throw new CompilerException("Can not redefine base relation.  Base relation with name " + head.getPredicateName() + " and arity " + head.getArity() + " already exists.");

		return this.activeModule.addRule(rule);
	}

	public boolean addFact(Predicate fact) {
		if (!fact.isGround())
			throw new RelationManagerException("Fact must be ground to add to the database.");
			
		return this.activeModule.addFact(fact);
	}

	public boolean addBasePredicate(BasePredicate relation) {
		return this.activeModule.addBasePredicate(relation);
	}

	public boolean addBasePredicateKey(BasePredicateKey key) {
		return this.activeModule.addBasePredicateKey(key);
	}
	
	public boolean addBasePredicateIndex(BasePredicateIndex index) {
		return this.activeModule.addBasePredicateIndex(index);
	}

	// if active module is removed, default module becomes active module
	// if default module is removed, default module is cleared
	public void removeModule(String moduleName) {
		Module moduleToRemove = null;

		for (Module module : this.modules) {
			if (module.getModuleName().equals(moduleName)) {
				moduleToRemove = module;
				break;
			}
		}

		// found no matching module, so exit without change
		if (moduleToRemove == null)
			return;

		if (moduleToRemove == this.activeModule)
			this.activeModule = this.defaultModule;
		
		// never remove the default module, just reset it
		if (moduleToRemove.getModuleName().equals(DEFAULT_MODULE_NAME)) {
			this.defaultModule.reset();
		} else {			
			this.modules.remove(moduleToRemove);
			moduleToRemove.reset();
		}
	}

	public void removeAllModules() {
		for (int i = this.modules.size() - 1; i >= 1; i--)
			this.modules.remove(i);

		this.defaultModule.reset();		
		this.activeModule = this.defaultModule;
	}

	public Module getModule(String moduleName) {
		for (Module m : this.modules) {
			if (m.getModuleName().equals(moduleName))
				return m;
		}
		return null;
	}

	public String toStringModuleNames() {
		StringBuilder retval = new StringBuilder();
		
		for (int i = 0; i < this.modules.size(); i++) {
			if (i > 0)
				retval.append(", ");
			retval.append(this.modules.get(i).getModuleName().toString());
		}
		
		return retval.toString();
	}

	public String toStringModules() {
		StringBuilder retval = new StringBuilder();

		for (Module module : this.modules) {
			if (retval.length() > 0)
				retval.append("\n");
			retval.append(module.toString());		
		}
					
		return retval.toString();
	}
	
	public String toJsonModules() {
		StringBuilder retval = new StringBuilder();
		retval.append("{\"modules\" : [");
		int counter = 0;
		for (Module module : this.modules) {
			if (counter > 0)
				retval.append(", ");
			retval.append(module.toJson());
			counter++;
		}
		retval.append("]}");
		return retval.toString();
	}
}
