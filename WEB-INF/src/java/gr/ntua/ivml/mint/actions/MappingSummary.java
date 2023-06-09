package gr.ntua.ivml.mint.actions;


import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Mapping;
import gr.ntua.ivml.mint.persistent.Organization;





import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import gr.ntua.ivml.mint.db.DB;

import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Result;
import org.apache.struts2.convention.annotation.Results;




@Results({
	  @Result(name="input", location="mappingsummary.jsp"),
	  @Result(name="error", location="mappingsummary.jsp"),
	  @Result(name="success", location="mappingsummary.jsp" )
	})

public class MappingSummary extends GeneralAction {
	public static final Logger log = Logger.getLogger(MappingSummary.class );
	
	
	private long orgId=-1;
	private long uploadId;
	private List<Mapping> recentMappings = new ArrayList<Mapping>();
	
	
	
	public long getUploadId() {
		return uploadId;
	}

	public void setUploadId(long uploadId) {
		this.uploadId = uploadId;
	}
	
	public long getOrgId() {
		if(orgId==-1){orgId=DB.getDatasetDAO().getById(this.getUploadId(), false).getOrganization().getDbID();}
		return orgId;
	}

	public void setOrgId(long orgId) {
		this.orgId = orgId;
	}


	public List<Organization> getOrganizations() {
		return  user.getAccessibleOrganizations();
		
	}
	
	private void findRecentMappings() {
		List<Mapping> maplist = getUser().getAccessibleMappings( true );
		if(this.getUploadId() > 0) {
			Dataset dataset = DB.getDatasetDAO().findById(this.getUploadId(), false);
			recentMappings = Mapping.getRecentMappings(dataset, maplist);
		} else {
			recentMappings = null;
		}
	}
	
	public List<Mapping> getRecentMappings() {
	return recentMappings;
}

public void setRecentMappings(List<Mapping> recentMappings) {
	this.recentMappings = recentMappings;
}
	
	@Action("MappingSummary")
	public String execute() {
		Organization o = user.getOrganization();
		// you are allowed to view nothing
		if( o == null ) return "success";
		
		Dataset du = DB.getDatasetDAO().findById(uploadId, false);
		if( user.can( "view data", user.getOrganization())  ||
			 user.sharesProject(du)) {
			if( !du.getItemizerStatus().equals(Dataset.ITEMS_OK)) {
				
				addActionError("You must first define the Item Level and Item Label.");
				return ERROR;
			}
		
			return "success";}
		else {
			addActionError("No rights to access mappings");
			return ERROR;
		}
			
	}
	
}
