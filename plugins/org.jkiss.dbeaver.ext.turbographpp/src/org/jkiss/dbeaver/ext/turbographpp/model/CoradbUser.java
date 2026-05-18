package org.jkiss.dbeaver.ext.turbographpp.model;

import org.jkiss.dbeaver.ext.generic.model.GenericSchema;

public class CoradbUser extends GenericSchema {
	
	private final String comment;

	public CoradbUser(TurboGraphPPDataSource dataSource, String name, String comment) {
		super(dataSource, null, name);
		this.comment = comment;
	}
	
	@Override
	public TurboGraphPPDataSource getDataSource() {
		return (TurboGraphPPDataSource) super.getDataSource();
	}
	
	public String getComment() {
		return comment;
	}
	

}
