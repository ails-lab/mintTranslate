package gr.ntua.ivml.mint.actions;

import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Organization;
import gr.ntua.ivml.mint.persistent.User;
import org.apache.log4j.Logger;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Result;
import org.apache.struts2.convention.annotation.Results;

import java.util.List;

@Results({
    @Result(name="input", location="enrichmentselection.jsp" ),
    @Result(name="error", location="enrichmentselection.jsp" )
})
public class Enrich extends GeneralAction  {
    protected final Logger log = Logger.getLogger(getClass());
    private long uploadId;
    private long organizationId;
    private long selectedEnrichment;
    private boolean noitem = false;


    @Action("Enrich_input")
    @Override
    public String input() throws Exception {

        log.debug("Enrichment selection controller");
        if( (user.getOrganization() == null && !user.hasRight(User.SUPER_USER)) || !user.hasRight(User.MODIFY_DATA)) {
            addActionError("No enrichment rights");
            return ERROR;
        }

        Dataset du = DB.getDatasetDAO().getById(getUploadId(), false);

        if (!du.getItemizerStatus().equals(Dataset.ITEMS_OK)) {
            this.noitem = true;
            addActionError("No item level and label defined.");
            return ERROR;
        }

        return "input";
    }

    //Returns all the organizations that a user can access, taking into account parent orgs.
    public List<Organization> getOrganizations() {
        return  user.getAccessibleOrganizations();
    }

    public void setOrganizationId(long orgId) {
        this.organizationId = orgId;
    }

    public long getOrganizationId() {
        Dataset du = DB.getDatasetDAO().getById(getUploadId(), false);
        if( du != null ) return du.getOrganization().getDbID();
        else return(this.organizationId);
    }

    public long getUploadId() {
        return uploadId;
    }

    public void setUploadId(long uploadId) {
        this.uploadId = uploadId;
    }

    public long getSelectedEnrichment() {
        return selectedEnrichment;
    }

    public void setSelectedEnrichment(long selectedEnrichment) {
        this.selectedEnrichment = selectedEnrichment;
    }
}
