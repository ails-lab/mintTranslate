<%@ taglib prefix="s" uri="/struts-tags" %>
<%@ page import="gr.ntua.ivml.mint.persistent.Organization" %>
<%@ page import="gr.ntua.ivml.mint.persistent.Mapping" %>
<%@ page import="gr.ntua.ivml.mint.util.Config" %>
<%@ page import="gr.ntua.ivml.mint.db.DB" %>
<%@ page import="org.apache.log4j.Logger" %>

<script type="text/javascript">

var mapurl="MappingOptions_input.action?";

function recentMappingsByDS() {
	var recent = $("<div>");
	
	$.get("Recent", {
		type: "mappingbyorg",
		organization: ooid,
		dataset:uId
	}, function(result) {			
		if(result.mappings != undefined && result.mappings.length>0) {
			
			recent.append($("<div id='recents'>").addClass("summary").append($("<div>").addClass("label").text("Recent mappings")));
			
			for(var i in result.mappings) {
				var entry = result.mappings[i];
				var div = kTemplater.jQuery('line.navigation', {
					label : "Mapping: " + entry.mapping.name,
					data : { kConnector:"html.page", url:"MappingOptions.action?uploadId=" + entry.dataset.dbID + "&selaction=editmaps&selectedMapping=" + entry.mapping.dbID, kTitle:"Mapping" }
				}).css({ height: "50px" }).appendTo(recent);
				div.append($("<div>").addClass("label").css({ top: "20px" }).text("Dataset: " + entry.dataset.name));
			}
		}
		
	}, "json");
	
	return recent;
}

$(function(){
	var panellast=$('div[id^="kp"]:last');
   if($("#recents").length==0){
	var mapdiv = $("#mappings-panel-mappings");
	
	mapdiv.prepend(recentMappingsByDS());
   }
	var parenttitle=panellast.find('.titlebar > table > tbody > tr > td.center > div.title').html();
   
	
if(parenttitle=='Transform'){
  
    mapurl='Transform.action';
   
    }

})
</script>
<div id="nmappings" style="display:none;"><s:property value="mappingCount"/></div>
<s:if test="hasActionMessages()">
		<s:iterator value="actionMessages">
			<div class="summary">
             <div class="info"><font color="red"><s:property escapeHtml="false" /> </font> </div>
         </div>  
		</s:iterator>
	</s:if>	
	<div class="summary">
	</div>

		
       
        <s:if test="accessibleMappings.size>0">
        
        <s:set var="lastOrg" value=""/>
         <s:iterator var="smap" value="accessibleMappings">
	         <s:set var="current" value="organization.dbID"/>
	         <s:if test="#current!=#lastOrg">
	                <div class="items separator">
	                
	                
					
					<div class="head">
					  <img src="images/museum-icon.png" width="25" height="25" style="left:1px;top:4px;position:absolute;max-width:25px;max-height:25px;"/>
					</div>
					
					<div class="label">Organization: <s:property value="organization.name"/></div>
					
					<div class="info"></div>
					
					</div>
					<s:set var="lastOrg" value="#current"/>
	         
	         </s:if>
	         
			<div title="<s:property value="name"/>" 
			onclick=" javascript:
			
			if(mapurl.indexOf('Transform.action')==-1){
			    var cp=$($(this).closest('div[id^=kp]'));$(cp).find('div.k-active').removeClass('k-active'); $(this).toggleClass('k-active');
				var loaddata={kConnector:'html.page', url:mapurl+'uploadId=<s:property value="uploadId"/>&selectedMapping=<s:property value="dbID"/>', kTitle:'Mapping options' };
           		$K.kaiten('removeChildren',cp, false);$K.kaiten('load', loaddata);}else{
           		importTransform(<s:property value="uploadId"/>,<s:property value="dbID"/>,<%=request.getParameter("orgId")%>);
           		}" 
			 class="items navigable">
			          	
			 				<div class="label" style="width:80%">						
							<s:property value="name"/> <font style="font-size:0.9em;margin-left:5px;color:grey;">(<s:property value="targetSchema"/>)</font></div>
							<s:if test="xsl != null && xsl==true"><span style="color:#a00">XSL</span></s:if>
							<div class="info">
							<s:if test="isLocked(user, sessionId)">
							<img src="images/locked.png" title="locked mappings" style="top:4px;position:relative;max-width:18px;max-height:18px;padding-right:4px;">
							</s:if>
							<s:if test="isShared()">
							<img src="images/shared.png" title="shared mappings" style="top:4px;position:relative;max-width:18px;max-height:18px;">
							</s:if>
							</div>
							<div class="tail"></div>
						</div>		
		</s:iterator>
		
      
       </s:if>
       
       <!--  #mappings-panel-mappings  -->


