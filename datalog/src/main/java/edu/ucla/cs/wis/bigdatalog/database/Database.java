package edu.ucla.cs.wis.bigdatalog.database;

import java.io.Serializable;
import java.nio.file.Path;

import edu.ucla.cs.wis.bigdatalog.database.cursor.CursorManager;
import edu.ucla.cs.wis.bigdatalog.database.index.IndexManager;
import edu.ucla.cs.wis.bigdatalog.database.type.TypeManager;
import edu.ucla.cs.wis.bigdatalog.system.DeALSContext;

public class Database implements Serializable {
	private static final long serialVersionUID = 1L;	
	private String 			databaseName;
	private String 			fileName;
	private RelationManager 	relationManager;
	private TypeManager 		typeManager;
	private IndexManager		indexManager;
	private CursorManager		cursorManager;
	
	public Database(String databaseName, DeALSContext deALSContext) {
		this(databaseName, null, deALSContext);
	}
	
	public Database(String databaseName, String fileName, DeALSContext deALSContext) {
		this.relationManager 	= new RelationManager(deALSContext.getConfiguration(), this);
		this.typeManager 		= new TypeManager(deALSContext.getConfiguration());
		this.databaseName 		= databaseName;
		this.fileName 			= fileName;
		this.indexManager		= new IndexManager(deALSContext);
		this.cursorManager		= new CursorManager(this);
	}

	public RelationManager getRelationManager() { return this.relationManager; }
	
	public void setRelationManager(RelationManager relationManager) {
		this.relationManager = relationManager;
	}
	
	public TypeManager getTypeManager() { return this.typeManager; }

	public void setString(String databaseName) {
		this.databaseName = databaseName;		
	}
	
	public String getDatabaseName() { return this.databaseName; }
	
	public String getFileName() { return this.fileName; }
	
	public IndexManager getIndexManager() { return this.indexManager; }
	
	public CursorManager getCursorManager() { return this.cursorManager; }
		
	public void clear() {
		this.relationManager.clear();
		this.typeManager.clear();
		this.indexManager.clear();
		this.cursorManager.clear();
	}
	
	public boolean save(Path filePath) {
		boolean status = this.relationManager.save(filePath);
		if (status)
			this.fileName = filePath.getFileName().toString();
		return status;
	}
	
	public String toString() {
		StringBuilder retval = new StringBuilder();
		retval.append("Database Name = ");
		retval.append(this.databaseName);
		retval.append("\nFilename = ");
		retval.append(this.fileName);
		return retval.toString();
	}
}
