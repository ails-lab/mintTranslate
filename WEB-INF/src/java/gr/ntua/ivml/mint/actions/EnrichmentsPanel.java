package gr.ntua.ivml.mint.actions;

import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.*;
import org.apache.log4j.Logger;
import org.apache.struts2.convention.annotation.Action;
import org.apache.struts2.convention.annotation.Result;
import org.apache.struts2.convention.annotation.Results;


import java.util.ArrayList;
import java.util.List;

@Results( { @Result(name = "input", location = "enrichmentsPanel.jsp"),
        @Result(name = "error", location = "enrichmentsPanel.jsp"),
        @Result(name = "success", location = "enrichmentsPanel.jsp"),
        @Result(name= "json", location="json.jsp" )
})
public class EnrichmentsPanel extends GeneralAction {
    protected final Logger log = Logger.getLogger(getClass());
    private long orgId;
    private int startEnrichment, endEnrichment, maxEnrichments;
    private int enrichmentCount = 0;
    private long uploadId;
    private Organization org = null;
    private List<Enrichment> accessibleEnrichments = new ArrayList<Enrichment>();


    public long getOrgId() {
        return orgId;
    }

    public void setOrgId(long orgId) {
        this.orgId = orgId;
        this.org = DB.getOrganizationDAO().findById(orgId, false);
    }

    public long getUploadId() {
        return uploadId;
    }

    public void setUploadId(long uploadId) {
        this.uploadId = uploadId;
    }

    public int getStartEnrichment() {
        return startEnrichment;
    }

    public void setStartEnrichment(int startEnrichment) {
        this.startEnrichment = startEnrichment;
    }

    public int getEndEnrichment() {
        return endEnrichment;
    }

    public void setEndEnrichment(int endEnrichment) {
        this.endEnrichment = endEnrichment;
    }

    public void setMaxEnrichments(int maxEnrichments) {
        this.maxEnrichments = maxEnrichments;
    }

    public int getMaxEnrichments() {
        return maxEnrichments;
    }

    public int getEnrichmentCount() {
        return enrichmentCount;
    }

    public void setEnrichmentCount(int enrichmentCount) {
        this.enrichmentCount = enrichmentCount;
    }

    public List<Enrichment> getAccessibleEnrichments() {
        return accessibleEnrichments;
    }

    public void findAccessibleEnrichments() {
        List<Enrichment> enrichments = new ArrayList<Enrichment>();
        try {
            if(this.orgId==-1){
                enrichments.addAll(getUser().getAccessibleEnrichments( true ));
            } else {
                enrichments.addAll(DB.getEnrichmentDAO().findByOrganization(this.org));
            }

            List<Enrichment> l = enrichments;
            this.enrichmentCount=enrichments.size();
            if(startEnrichment<0)startEnrichment=0;


            if(l.size()>(startEnrichment+maxEnrichments)){
                accessibleEnrichments = enrichments.subList((int)(startEnrichment), startEnrichment+maxEnrichments);}
            else{
                accessibleEnrichments = enrichments.subList((int)(startEnrichment),l.size());}
        }
        catch (Exception ex) {
            log.debug(" ERROR GETTING Enrichments:" + ex.getMessage());
        }
    }

    @Action("EnrichmentsPanel")
    public String execute() {
        if ((user.getOrganization() == null && !user.hasRight(User.SUPER_USER))
                || !user.hasRight(User.MODIFY_DATA)) {
            addActionError("You dont have rights to access mappings.");
            return ERROR;
        }
        findAccessibleEnrichments();
        return "success";
    }
}