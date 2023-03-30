var enrichmentlimit=20;
var uId;
var enrichment_oid;
var enrichment_uid;

function ajaxEnrichPanel(from, organizationId, userId, uploadId) {
    if(uploadId==null){uploadId=-1};
    enrichment_uid=userId;
    uId=uploadId;
    enrichment_oid=organizationId;

    $.ajax({
        url: "EnrichmentsPanel",
        type: "POST",
        data: "startEnrichment=" + from + "&maxEnrichments=" + enrichmentlimit + "&orgId=" + organizationId + "&uploadId=" + uploadId,
        error: function(){
            alert("An error occured. Please try again.");
        },
        success: function(response){
            $("div[id=enrichmentPanel]").html( response);
            if(from==0){
                var numentries=$("div[id=nmappings]").text();
                initEnrichmentPagination(numentries);
            }
        }
    });

}

/**
 * Callback function for the AJAX content loader.
 */
function initEnrichmentPagination(num_entries) {

    // Create pagination element
    $(".enrichments_pagination").pagination(num_entries, {
        num_display_entries:7,
        num_edge_entries: 1,
        callback: enrichmentsSelectCallback,
        load_first_page:false,
        items_per_page:mappinglimit
    });
}

function enrichmentsSelectCallback(page_index,jq){
    /*find start, end from page_index*/
    end=(page_index+1)*mappinglimit;
    start=end-(mappinglimit);
    ajaxEnrichPanel(start,enrichment_oid,enrichment_uid,uId);
    return false;
}

