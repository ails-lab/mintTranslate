<%@ taglib prefix="s" uri="/struts-tags" %>
<%@ page import="gr.ntua.ivml.mint.persistent.Organization" %>
<%@ page import="gr.ntua.ivml.mint.persistent.Mapping" %>
<%@ page import="gr.ntua.ivml.mint.util.Config" %>
<%@ page import="gr.ntua.ivml.mint.db.DB" %>
<%@ page import="org.apache.log4j.Logger" %>

<div id="nmappings" style="display:none;"><s:property value="enrichmentCount"/></div>
<s:if test="hasActionMessages()">
    <s:iterator value="actionMessages">
        <div class="summary">
            <div class="info"><font color="red"><s:property escapeHtml="false" /> </font> </div>
        </div>
    </s:iterator>
</s:if>
<div class="summary">
</div>
<s:if test="accessibleEnrichments.size>0">
    <s:set var="lastOrg" value=""/>
    <s:iterator var="enrich" value="accessibleEnrichments">
        <s:set var="current" value="orgId"/>
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
        <div   title="<s:property value="name"/>"
<%--             onclick=" javascript:--%>

<%--                     if(mapurl.indexOf('Transform.action')==-1){--%>
<%--                     var cp=$($(this).closest('div[id^=kp]'));$(cp).find('div.k-active').removeClass('k-active'); $(this).toggleClass('k-active');--%>
<%--                     var loaddata={kConnector:'html.page', url:mapurl+'uploadId=<s:property value="uploadId"/>&selectedMapping=<s:property value="dbID"/>', kTitle:'Mapping options' };--%>
<%--                     $K.kaiten('removeChildren',cp, false);$K.kaiten('load', loaddata);}else{--%>
<%--                     importTransform(<s:property value="uploadId"/>,<s:property value="dbID"/>,<%=request.getParameter("orgId")%>);--%>
<%--                     }"--%>
                onclick="javascript:
                            <%--var enrichmentId = <s:property value="dbID"/>--%>
                            <%--var uploadId = <s:property value="uploadId"/>--%>
                            <%--var text = 'EnrichmentId=' + enrichmentId + ' And UploadId=' + uploadId + ' is ready to run...';--%>
                            <%--alert(text);--%>
                         var cp=$($(this).closest('div[id^=kp]'));
                         $K.kaiten('removeChildren',cp, false);
                         $K.kaiten('reload',cp,{kConnector:'html.page', url:'enrichmentDefineXpaths?uploadId=<s:property value="uploadId"/>&enrichmentId=<s:property value="dbID"/>', kTitle:'Define Xpaths for Enrichment' });"
                class="items navigable">
            <div class="label" style="width:80%">
                <s:property value="name"/><font style="font-size:0.9em;margin-left:5px;color:grey;">(<s:property value="creationDate"/>)</font></div>
            <div class="info"></div>
            <div class="tail"></div>
        </div>
    </s:iterator>
</s:if>