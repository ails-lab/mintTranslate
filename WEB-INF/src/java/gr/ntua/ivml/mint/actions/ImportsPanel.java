
package gr.ntua.ivml.mint.actions;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Result;
import org.apache.struts2.convention.annotation.Results;

import gr.ntua.ivml.mint.Publication;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Organization;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.util.Jackson;
import gr.ntua.ivml.mint.view.Import;

@Results({
	@Result(name="error", location="importsPanel.jsp"),
	@Result(name="success", location="importsPanel.jsp")
})

public class ImportsPanel extends GeneralAction{

	protected final Logger log = Logger.getLogger(getClass());

	
	private int startImport, maxImports;
	private long orgId;
	private long projectId;
	private long userId=-1;
    private String actionmessage="";
    private Organization o=null;
    private String labels="";
    private long dsId = -1;
    private int currentPage;
    private int importCount = 0;
    
    private String[] folders;
    
	private List<Import> result = new ArrayList<Import>();

	public static class EmbeddedJsonData {
		public int currentPage;
		public int importCount;
		public long projectId;
		public long orgId;
		public String[] folders;
	}
	
	@Action(value="ImportsPanel")
	public String execute() throws Exception {
		// can user request stuff
		// which filters we need: project / org / folders
		// call db to get datasets with start and limit and these filters
		
		Organization org = DB.getOrganizationDAO().getById(orgId, false);
		if( org == null ) {
			addActionError("No organization found. Please register to an existing organization by updating your 'Account' or create a new orginization using the 'Administration' option.");
			return "error";
		}
		
		if( maxImports > 0 )
			currentPage = startImport / maxImports;
		else {
			addActionError( "Paging problem, maxImports <= 0 ");
			return "error";
		}
		
		User u = getUser();
		List<Dataset> l = null;
		folders = labels.length()>0 ? labels.split(",") : new String[0];
		
		if( u.can( "view data", org )) {
			// user has access via org
			if( projectId > 0 ) {
				//find by project labels and org
				l = DB.getDatasetDAO().findNonDerivedByProjectFolders(org, projectId, folders);
			} else {
				// just by org and labels
				l = DB.getDatasetDAO().findNonDerivedByOrganizationFolders( org, folders);
			}
		}
		else if( u.sharesProject(org)) { 
			// user has project access
			// projectId is mandatory
			l = DB.getDatasetDAO().findNonDerivedByProjectFolders(org, projectId, folders);
		}
		else {
			// user has no access 
			addActionError( "No access");
			return( "error");
		}
		
		if(( l != null ) && ( l.size()>0 )) {
			pageResults( l );
			importCount= l.size();
		}
		return SUCCESS;
	}

	// adds the rights elements to the result
	private void pageResults( List<Dataset> lds ) {
		   if(startImport<0)startImport=0;
			while(lds.size()<=startImport){
				startImport=startImport-maxImports; 
			 }
			
		    if(lds.size()>(startImport+maxImports)){	
		    	lds = lds.subList((int)(startImport), startImport+maxImports);}
		    else{
		    	lds = lds.subList((int)(startImport),lds.size());}
		    
		    for( Dataset x: lds ) {
					Import su = new Import(x);
					result.add(su);
			}
	}
	
	 public String getActionmessage(){
		  return(actionmessage);
		  
	  }
	  
	  public void setActionmessage(String message){
		  this.actionmessage=message;
		  
	  }
	
	  public String getLabels(){
		  return(labels);
		  
	  }
	  
	  public void setLabels(String labels){
		  this.labels=labels;
		  
	  }	

	
	public long getDsId() {
		return dsId;
	}

	
	public void setDsId(long dsId) {
		this.dsId = dsId;
	}


	public String getPstatusIcon(){
		Publication p = o.getPublication();
			if( Dataset.PUBLICATION_OK.equals( p.getStatus())) 
				return "images/okblue.png";
			else if( Dataset.PUBLICATION_FAILED.equals( p.getStatus())) 
				return "images/problem.png";
			else if( Dataset.PUBLICATION_RUNNING.equals( p.getStatus())) 
				return "images/loader.gif";
		return "images/spacer.gif";
	}
	
	
	public String getPstatus(){
		String result =  getO().getPublication().getStatus();
		return result;
	}
	
	
	public int getStartImport() {
		return startImport;
	}

	public void setStartImport( int startImport ) {
		this.startImport = startImport;
	}


	public int getMaxImports() {
		return maxImports;
	}

	public void setMaxImports(int maxImports) {
		this.maxImports = maxImports;
	}


	public long getOrgId() {
		return orgId;
	}

	public void setOrgId(long orgId) {
		this.orgId = orgId;
		this.o=DB.getOrganizationDAO().findById(orgId, false);
	}

   
	public long getUserId() {
		return userId;
	}

	public Organization getO(){
		return this.o;
	}
	

	public List<Import> getImports() {
		return result;
	}
	
	
	/**
	 * Return on which page is 
	 * @return
	 */
	public int getCurrentPage() {
		return currentPage;
	}
	
	public int getImportCount() {
		return importCount;
	}


	public long getProjectId() {
		return projectId;
	}


	public void setProjectId(long projectId) {
		this.projectId = projectId;
	}


	public String[] getFolders() {
		return folders;
	}

	public String getJsonData() {
		EmbeddedJsonData js = new EmbeddedJsonData();
		js.currentPage = currentPage;
		js.folders = folders;
		js.importCount = importCount;
		js.orgId= orgId;
		js.projectId = projectId;
		try {
			return Jackson.om().writeValueAsString(js);
		} catch( Exception e ) {
			log.error( "", e );
			return "{}";
		}
	}
	
	
	public void setFolders(String[] folders) {
		this.folders = folders;
	}
}
