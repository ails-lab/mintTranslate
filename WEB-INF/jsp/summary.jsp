<%@ include file="_include.jsp"%>
<%@ page language="java" errorPage="error.jsp"%>
<%@ page import="gr.ntua.ivml.mint.util.Label" %>
<%@page pageEncoding="UTF-8"%> 
<%
response.setHeader("Cache-Control","no-store");//HTTP 1.1
response.setHeader("Pragma","no-cache"); //HTTP 1.0
response.setDateHeader ("Expires", -1);
%>


<%String orgId=(String)request.getParameter("orgId");%>

<div class="panel-body">
<div class="block-nav">
	
<div class="summary">
	<div class="label">My workspace</div>  
     <s:if test="hasActionErrors()">
		<s:iterator value="actionErrors">
			<div class="info"><div class="errorMessage"><s:property escapeHtml="false" /> </div></div>
		</s:iterator>
	</s:if>
	<s:else>		
    	<div class="info">
			An overview of all the datasets per organization and per uploader:
		</div>
	</s:else>
	
</div>

<s:if test="!hasActionErrors()">
	
<%if(user.hasRight(User.MODIFY_DATA)) {
	if(user.hasRight(User.SUPER_USER) || !Config.getBoolean("ui.hide.import")) {
%>
		
	<div title="Import" data-load='{"kConnector":"html.page", "url":"Import_input.action", "kTitle":"Start Import" }' class="items navigable">
		<div class="label">Import new archive</div>
		<div class="tail"></div>
	</div>

	<div title="Import" data-load='{"kConnector":"html.page", "url":"Import_input_enrichment.action", "kTitle":"Start Import Enrichment" }' class="items navigable">
		<div class="label">Import new Enrichment</div>
		<div class="tail"></div>
	</div>
		
	<!-- Create an empty Dataset for annotator -->
	<div title="Create empty dataset" data-load='{"kConnector":"html.page", "url":"CreateDataset_input.action", "kTitle":"Create Dataset" }' class="items navigable">
		<div class="label">Create empty dataset</div>
		<div class="tail"></div>
	</div>
		
	<!-- label creation -->
		
	<div id="dialog-lblmanage" title="Manage labels" >
		<button id="btn_labeladd" onclick="addlabel()">Add Label</button><br/><br/>
		
		<form id="formlabel">
		</form>
	</div>
		
<% }
} %>
	<div class="summary">  </div>
	<s:if test="organizations.size()>1">
		<div class="mint2-action">
			<span class="selector-text" style="width:150px;display:inline-block"><b>Filter by Organization : </b></span>
			 <s:select class="selector" theme="simple" cssStyle="width:200px" name="filterorg" id="filterorg" list="organizations" listKey="dbID" listValue="name" value="orgId"  onChange="var cp=$($(this).closest('div[id^=kp]'));$K.kaiten('reload',cp,{ kConnector:'html.page', url:'ImportSummary.action?orgId='+$('#filterorg').val(), kTitle:'My workspace' },false);"></s:select>
		</div>
	</s:if>
	<s:else>
		<div class="mint2-action" >
			<span class="selector-text" style="width:150px;display:inline-block"><b>Organization : </b></span>
			<span class="selector-text"> <s:property value="organizations.get(0).englishName" /> </span>
			<input type="hidden" id="filterorg" value="<s:property value="organizations.get(0).dbID"/>" />
		</div>
	</s:else>
		
	<s:if test="projects.size()>0">
		<div class="mint2-action">
			<span class="selector-text" style="width:150px;display:inline-block"><b>Filter by Project : </b></span>
			<s:select class="selector" theme="simple"  cssStyle="width:200px"  name="filterproj"  id="filterproj" list="projects" listKey="dbID" listValue="name" value="projectId"  onChange="javascript:ajaxImportsPanel(0,importlimit,$('#filterproj').val(),$('#filterorg').val(), $('#filterlabel').val());"></s:select>
		</div>
	</s:if>  
	
	<div class="mint2-action">
		<s:if test="labels.size()>0">
			<span class="selector-text" style="width:150px;display:inline-block"><b>Filter by Label :</b> </span>
			<s:select class="selector" theme="simple"  cssStyle="width:200px" name="filterlabel"  id="filterlabel" list="labels" headerKey="-1" headerValue="-- All labels --" listKey="%{lblname+'_'+lblcolor}"  listValue="lblname"  onChange="javascript:ajaxImportsPanel(0,importlimit,$('#filterproj').val(),$('#filterorg').val(), $('#filterlabel').val());" multiple="true"></s:select>
		</s:if>
		<s:if test="labels.size()==0">
			<span class="selector-text-hidden"><b>Set labels for organization :</b> </span>
		</s:if>
		<div class="labelset lbutton" id="labels_set" title="Set labels for organization" style="top:1em;"></div>						
	</div>
	
	<div class="summary">  </div>
	
	<div class="summary" id="searchsets">      
		<div></div>
	</div>
          
	<div class="summary">  </div>
	<div class="imports_pagination"></div>

	<div id="importsPanel"> 
		<script> ajaxImportsPanel(0,importlimit,$('#filterproj').val(),$('#filterorg').val(), $('#filterlabel').val()); </script>
    </div>
     
     <div class="imports_pagination"></div>
    
</s:if> <!--  Action errors -->

</div>
    <script>
     	$(document).ready(function () {
     		$("#filterproj").chosen();
     		// filterorg might be a hidden input instead of a selector
     		
     		$("#filterorg").filter("select").chosen();
     		$("#filterlabel").chosen();
     		
     		var panelcount=$('div[id^="kp"]:last');
			var panelid=panelcount.attr('id');
			var pnum=parseInt(panelid.substring(2,panelid.length));
			
	    	newpanel=$("#kp2");
	    	
			if(pnum==1){ 	
				
				newpanel=$("#kp1"); 	
			}else{
				newpanel=$("#kp2");
			}
			
			$( "#dialog-lblmanage" ).dialog({
     			autoOpen: false,
     			height: 300,
     			width: 600,
     			modal: true,
     			buttons: {
     			Close: function() {
     			  $( this ).dialog( "close" );
     				
     			}
     			},
     			close: function() {
     				$('[id^="color"]').colorpicker("destroy");
     				labelelems=0;
     			 	$("#formlabel").empty();
     			 	 $K.kaiten('reload',newpanel,{kConnector:'html.page', url:'ImportSummary.action?&orgId='+$('#filterorg').val(), kTitle:'My workspace' });
               		
     			}
     		});

			
			this.searchContainer = Mint2.searchBox({
				prompt : "Search your items",
				callback : function(term) {
					searchForSets(0, importlimit, term,<%=user.getDbID()%>,$('#filterorg').val());
				}
			}).appendTo('#searchsets');
	
			
           $("#labels_set").click(function() {
     		$( '#dialog-lblmanage' ).dialog( 'open' );$('#btn_labeladd').button({icons: {primary: 'ui-icon-circle-plus'},text: true});getlabels();var cp=$($(this).closest('div[id^=kp]'));$(cp).find('div.k-active').removeClass('k-active');$(this).toggleClass('k-active');$K.kaiten('removeChildren',cp, false);
           })	
     	});
     </script>
</div>


