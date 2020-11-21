package edu.ucla.cs.wis.bigdatalog.system;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import edu.ucla.cs.wis.bigdatalog.exception.DeALSException;
import edu.ucla.cs.wis.bigdatalog.type.DataType;

public class DeALSConfiguration implements Serializable {
	private static final long serialVersionUID = 1L;
	private static Logger logger = LoggerFactory.getLogger(DeALSConfiguration.class.getName());
	public static String configFileName = "deals.properties";

	private		Properties 		properties;
	protected 	DataType		countDataType;
	
	public DeALSConfiguration() {
		this.properties = getProperties(configFileName);
		this.initialize();
	}
	
	private void initialize() {
		switch (Integer.valueOf(this.getProperty("deals.countintegersize"))) {
			case 64:
				this.countDataType = DataType.LONG;
				break;
			case 128:
				this.countDataType = DataType.LONGLONG;
				break;
			case 256:
				this.countDataType = DataType.LONGLONGLONGLONG;
				break;
			default:
				this.countDataType = DataType.INT;
		}
	}
	
	public DataType getCountDataType() { return this.countDataType; }
	
	public String getProperty(String key) { return this.properties.getProperty(key); }
	
	public int getPropertyInt(String key) { return Integer.parseInt(this.properties.getProperty(key)); }
	
	public boolean compareProperty(String key, String expectedValue) { 
		return this.properties.getProperty(key).equals(expectedValue); 
	}
	
	public void setProperty(String key, String value) { 
		this.properties.put(key, value);
		
		this.initialize();
	}
	
	public void setProperties(Map<String, String> properties) {
		if (properties == null)
			return;
		
		for (Entry<String, String> entry : properties.entrySet()) 
			this.setProperty(entry.getKey(), entry.getValue());
	}
	
	public void clear() { this.properties.clear(); }

	public String toString() {
		StringBuilder output = new StringBuilder();
		
		// alphabetize - we know they're Strings, not Objects
		List<Object> propertyKeys = new ArrayList<>(this.properties.keySet());
		
		Collections.sort(propertyKeys, new Comparator<Object>() {
	        public int compare(Object s1, Object s2) {
	            return ((String)s1).compareTo((String)s2);
	        }
	    });
		
		for (Object propertyKey : propertyKeys) {
			if (output.length() > 0) output.append("\n");
			output.append(propertyKey + " = " + this.properties.get(propertyKey));
		}
		
		return output.toString();
	}
	
	public static Properties getProperties(String filepath) {
		FileInputStream fis = null;
		Properties properties = null;
		try {
			properties = new Properties(System.getProperties());
			if (Files.exists(Paths.get(filepath), LinkOption.NOFOLLOW_LINKS)) {
				fis = new FileInputStream(filepath);
				properties.load(fis);
				//System.out.println("Using deals.properties in root.");
			} else {
				ClassLoader classloader = Thread.currentThread().getContextClassLoader();
				properties.load(classloader.getResourceAsStream(filepath));
				//System.out.println("Using deals.properties from resources.");
			}
		} catch (FileNotFoundException e) {
			logger.error(e.getMessage());
			throw new DeALSException("Can not initialize DeALS without "+filepath+" file. " + e.getMessage());
		} catch (IOException e) {
			logger.error(e.getMessage());
			throw new DeALSException("Can not initialize DeALS without "+filepath+". " + e.getMessage());
		} 
		return properties;
	}	
}
