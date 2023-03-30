package gr.ntua.ivml.mint.actions;

import java.io.File;

import org.apache.log4j.Logger;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Result;
import org.apache.struts2.convention.annotation.Results;

import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.Enrichment;
import gr.ntua.ivml.mint.persistent.Organization;

@Results({
        @Result(name="success", location="successimport.jsp"),
        @Result(name="error", location="enrichment_import.jsp")
})

public class ImportEnrichment extends GeneralAction {

    protected final Logger log = Logger.getLogger(getClass());
    private long uploaderOrg;
    private String upfile;
    private String httpUp;
    private String method;

    @Action("ImportEnrichment")
    public String execute() throws Exception {
        log.info( "Import Enrichment Action");
//        log.info(getHttpup()+getMth()+getUpfile()+getUploaderOrg());
        Enrichment enrichment = new Enrichment();
        File upload = new File(System.getProperty("java.io.tmpdir"),this.upfile);
        boolean success = enrichment.initialize(upload, httpUp, user);
        if (success) {
        	Organization org = DB.getOrganizationDAO().getById(getUploaderOrg(), false);
        	if( org != null ) enrichment.setOrganization(org);
            DB.getEnrichmentDAO().makePersistent(enrichment);
            DB.commit();
            return "success";
        }
        else {
            return "error";
        }
    }

    //Boiler-plate code
    public long getUploaderOrg() {
        return uploaderOrg;
    }

    public void setUploaderOrg(long uploaderOrg) {
        this.uploaderOrg = uploaderOrg;
    }

    public String getUpfile() {
        return upfile;
    }

    public void setUpfile(String upfile) {
        this.upfile = upfile;
    }

    public String getHttpup() {
        return httpUp;
    }

    public void setHttpup(String httpUp) {
        this.httpUp = httpUp;
    }

    public String getMth() {
        return method;
    }

    public void setMth(String method) {
        this.method = method;
    }
}
