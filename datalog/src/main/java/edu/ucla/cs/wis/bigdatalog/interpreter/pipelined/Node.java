package edu.ucla.cs.wis.bigdatalog.interpreter.pipelined;

import java.lang.reflect.Array;

import org.slf4j.LoggerFactory;

import edu.ucla.cs.wis.bigdatalog.compiler.variable.Binding;
import edu.ucla.cs.wis.bigdatalog.interpreter.Status;
import edu.ucla.cs.wis.bigdatalog.interpreter.argument.NodeArguments;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public abstract class Node<T extends edu.ucla.cs.wis.bigdatalog.interpreter.Node<?>> 
	extends edu.ucla.cs.wis.bigdatalog.interpreter.Node<T> {
	
	protected int 			currentChildIndex;
	protected boolean 		isEntry;
	protected DeALSContext 	deALSContext;
	
	public Node(String predicateName, NodeArguments args, Binding binding) {
		super(predicateName, args, binding);
		this.currentChildIndex	= 0;
		this.isEntry			= true;
	}

	public DeALSContext getDeALSContext() { return this.deALSContext; }
	
	public void baseNodeCleanUp() {
		this.isEntry = true;
		this.currentChildIndex = 0;
	}
	
	@SuppressWarnings("unchecked")
	protected T[] createArray(int size) {
		Class<?> clazz = AndNode.class;
		if (this instanceof AndNode)
			clazz = OrNode.class;

		return (T[])Array.newInstance(clazz, size);
	}
	
	public void traceGetTupleEntry() {		
		if (DEBUG && this.deALSContext.isTraceEnabled()) {
			/*StringBuilder output = new StringBuilder();
			if (this.isEntry)
				output.append("Entering ");
			else
				output.append("Backtracking into ");

			output.append(this.getClass().getSimpleName());
			output.append(".getTuple() : ");
			output.append(this.toStringNode());
			logger.trace(output.toString());*/
			
			if (this.isEntry)
				LoggerFactory.getLogger(this.getClass().getName()).trace("Entering {}.getTuple() : {}",
						this.getClass().getSimpleName(), this.toStringNode());
			else
				LoggerFactory.getLogger(this.getClass().getName()).trace("Backtracking into {}.getTuple() : {}",
						this.getClass().getSimpleName(), this.toStringNode());
			
	    }
	}

	public void traceGetTupleExit(Status status) {
		if (DEBUG && this.deALSContext.isTraceEnabled()) {
			/*StringBuilder output = new StringBuilder();
			output.append("Exiting ");
			output.append(this.getClass().getSimpleName());
			output.append(".getTuple() : ");
			output.append(this.toStringNode());
			output.append(" with status = ");
			output.append(status.getName());
			logger.trace(output.toString());*/
			LoggerFactory.getLogger(this.getClass().getName()).trace("Exiting {}.getTuple() : {} with status = {}",
					this.getClass().getSimpleName(), 
					this.toStringNode(), 
					status.getName());
			
	    }
	}
	
	protected void logTrace(String message, Object...objects) {
		if (this.deALSContext.isTraceEnabled())
			LoggerFactory.getLogger(this.getClass().getName()).trace(message, objects);
	}
	
	protected void logTrace(String message) {
		if (this.deALSContext.isTraceEnabled())
			LoggerFactory.getLogger(this.getClass().getName()).trace(message);
	}
	
	protected void logDebug(String message, Object...objects) {
		if (this.deALSContext.isDebugEnabled())
			LoggerFactory.getLogger(this.getClass().getName()).debug(message, objects);
	}
	
	protected void logDebug(String message) {
		if (this.deALSContext.isDebugEnabled())
			LoggerFactory.getLogger(this.getClass().getName()).debug(message);
	}
	
	protected void logInfo(String message, Object...objects) {
		if (this.deALSContext.isInfoEnabled())
			LoggerFactory.getLogger(this.getClass().getName()).info(message, objects);
	}
	
	protected void logInfo(String message) {
		if (this.deALSContext.isInfoEnabled())
			LoggerFactory.getLogger(this.getClass().getName()).info(message);
	}
	
	protected void logDerivationTracking(String message, Object...objects) {
		if (this.deALSContext.isDerivationTrackingEnabled())
			LoggerFactory.getLogger(this.getClass().getName()).info(message, objects);
	}
	
	protected void logDerivationTracking(String message) {
		if (this.deALSContext.isDerivationTrackingEnabled())
			LoggerFactory.getLogger(this.getClass().getName()).info(message);
	}
	
	protected void logError(String message) {
		if (this.deALSContext.isErrorEnabled())
			LoggerFactory.getLogger(this.getClass().getName()).error(message);
	}
	
	protected void logError(String message, Object...objects) {
		if (this.deALSContext.isErrorEnabled())
			LoggerFactory.getLogger(this.getClass().getName()).error(message, objects);
	}

	public Node<T> copy() {
		return this.copy(new ProgramContext());
	}	
	
	public void attachContext(DeALSContext deALSContext) {
		super.attachContext(deALSContext);
		this.deALSContext = deALSContext;
	}
	
	abstract public Node<T> copy(ProgramContext programContext);
}
