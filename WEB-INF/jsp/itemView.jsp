
<%@ include file="_include.jsp"%>
<%@ page language="java" errorPage="error.jsp"%>
<%@page pageEncoding="UTF-8"%> 

<div class="panel-body">
	<div class="block-nav">
	
		<s:if test="hasActionErrors()">
		    <div class="info">
				<s:iterator value="errorMessages">
		             <div class="info"><font color="red"><s:property escapeHtml="false" /> </font> </div>
				</s:iterator>
			</div>
	    </s:if>	
		
	    <s:if test="!hasActionErrors()">
	    	<div style="display: none">
	    		<div id="itemViewUploadId"><s:property value="uploadId"/></div>
	    		<div id="itemViewFilter"><s:property value="filter"/></div>
	    		<div id="itemViewItemId"><s:property value="itemId"/></div>
	    		<div id="itemViewQterm"><s:property value="query"/></div>
	    	</div>
	    	<div id="item-preview" style="margin: 10px">
	    	</div>
			<script>
				$(document).ready(function () {
					$("#item-preview").itemPreview({
						datasetId: $("#itemViewUploadId").text(),
						filter: $("#itemViewFilter").text(),
						itemId: $("#itemViewItemId").text(),
						query: $("#itemViewQterm").text()
					}).bind("beforePreviewItem", function() {
					    var panel = $("#item-preview").closest('div[id^="kp"]');
					    $K.kaiten("maximize", panel);
					});
					var itemId = $("#itemViewItemId").text();
					if( $("#itemViewQterm").text()){
						 $(".mint2-search-box>input").val( $("#itemViewQterm").text());}
					if( itemId != -1 ) {
					    var panel = $("#item-preview").closest('div[id^="kp"]');
						$("#item-preview").data("itemPreview").previewItem( itemId, null, [ "dataset,schema" ]);
					}
				});
			</script>
	    </s:if>
	</div>
</div>