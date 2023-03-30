package gr.ntua.ivml.mint.actions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.log4j.Logger;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Result;
import org.apache.struts2.convention.annotation.Results;

import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.Organization;
import gr.ntua.ivml.mint.persistent.Project;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.util.Label;

@Results({
	  @Result(name="input", location="summary.jsp"),
	  @Result(name="error", location="summary.jsp"),
	  @Result(name="success", location="summary.jsp" )
	})

public class ImportSummary extends GeneralAction {
	public static final Logger log = Logger.getLogger(ImportSummary.class );
	private String error="";
	
	
	long orgId;
	long projectId;
	long uploaderId;
	long linkDs = -1l;
	
	public long getOrgId() {
		return orgId;
	}


	public void setOrgId(long orgId) {
		this.orgId = orgId;
	}

	public long getProjectId() {
		return projectId;
	}


	public void setProjectId(long projectId) {
		this.projectId = projectId;
	}

	public long getUploaderId() {
		return uploaderId;
	}


	public void setUploaderId(long uploaderId) {
		this.uploaderId = uploaderId;
	}

	
	public List<Organization> getOrganizations() {
		return  user.getAccessibleOrganizations();
	}
	
	public List<Project> getProjects() {
		// only the projects of the organisation 
		Organization org = DB.getOrganizationDAO().findById(getOrgId(), false);
		List<Integer> projectIds = Collections.emptyList();
		ArrayList<Project> res = new ArrayList<>();
		
		if( org != null ) {
			if( !getUser().can( "view data", org )) {
				// if there is no project overlap, we are illegal
				projectIds = Project.sharedIds(
						getUser().getProjectIds(), 
						org.getProjectIds());
			} else {
				projectIds = org.getProjectIds();
			}
		
			List<Long> longIds = projectIds.stream()
				.map( i-> (long) i)
				.collect( Collectors.toList());
			
			res.addAll( DB.getProjectDAO().getByIds(longIds ));
			
			if( getUser().can( "view data", org )) {
				Project dummy = new Project();
				dummy.setDbID(-1l);
				dummy.setName( "---");
				res.add( 0, dummy );
			}
		}			
		return  res;
	}
	
	public List<User> getUploaders(){
		return DB.getOrganizationDAO().findById(orgId, false).getUploaders();
	}
	
	public List<Label> getLabels(){
		List<String> list = new ArrayList<String>((DB.getOrganizationDAO().getById(orgId, false)).getFolders());
		ArrayList<Label> labels=new ArrayList<Label>();
		for(String inputlbl:list){
			Label newlabel=new Label(inputlbl);
			labels.add(newlabel);
		}
		return labels;
		
	}
	
	public void setError(String error) {
		addActionError(error);
	}


	public String getError() {
		return StringEscapeUtils.escapeHtml(error);
	}

	
	public long getLinkDs() {
		return linkDs;
	}


	public void setLinkDs(long linkDs) {
		this.linkDs = linkDs;
	}


	@Action("ImportSummary")
	public String execute() {
		Organization o = user.getOrganization();
		// you are allowed to view nothing
		if( o == null ){
			setError("No organization found. Please register to an existing organization by updating your 'Account' or create a new orginization using the 'Administration' option.");
			return "success";
		}
		
		if( user.can( "view data", user.getOrganization() ))
			return "success";
		else {
			setError("You have no access to this area.");
			
			throw new IllegalAccessError( "No rights" );
		}
	}
	
}
