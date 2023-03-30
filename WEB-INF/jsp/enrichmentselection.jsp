<%@ include file="_include.jsp"%>
<%@ page language="java" errorPage="error.jsp"%>
<%@page pageEncoding="UTF-8"%>

<%--If the action gave us errors, display them on the summary!--%>
<s:if test="hasActionErrors()">

    <div class="panel-body">
        <div class="block-nav">
            <div class="summary">
                <div class="label">Error</div>
                <s:iterator value="actionErrors">
                    <font color="red"><s:property escapeHtml="false" /> </font><br/>
                </s:iterator>
            </div>
        </div>
    </div>
</s:if>
<s:elseif test="!hasActionErrors()">
    <div class="panel-body">
        <div class="block-nav">
            <div class="summary">
                <div class="label">
                    Select Enrichment</div>

                <div class="info">
                    <s:if test="hasActionErrors()">
                        <s:iterator value="errorMessages">
                            <div class="info"><font color="red"><s:property escapeHtml="false" /> </font> </div>
                        </s:iterator>
                    </s:if>
                </div>

            </div>



            <s:if test="organizations.size()>0">
                <div style="display:block;padding:5px 0 0 5px;background:#F2F2F2;border-bottom:1px solid #CCCCCC;">
                    <span style="width:150px;display:inline-block"><b>Filter by Organization: </b></span><s:select theme="simple"  cssStyle="width:200px"  name="filterenrichorg"  id="filterenrichorg" vaue="${organizationId}" list="organizations" listKey="dbID" listValue="name" headerKey="-1" headerValue="-- All enrichments --" onChange="javascript:ajaxEnrichPanel(0,$('#filterenrichorg').val(),${user.dbID},${uploadId});"></s:select>
                </div>
            </s:if>

<%--            <s:if test="recentMappings.size>0">--%>
<%--                <div id="mappings-panel-recent-mappings"  style="padding: 0">--%>
<%--                    <div class="summary"><div class="label">Relevant Mappings</div></div>--%>
<%--                    <s:set var="lastOrg" value=""/>--%>
<%--                    <s:iterator var="smap" value="recentMappings">--%>
<%--                        <s:set var="current" value="organization.dbID"/>--%>
<%--                        <s:if test="#current!=#lastOrg">--%>
<%--                            <div class="items separator">--%>



<%--                                <div class="head">--%>
<%--                                    <img src="images/museum-icon.png" width="25" height="25" style="left:1px;top:4px;position:absolute;max-width:25px;max-height:25px;"/>--%>
<%--                                </div>--%>

<%--                                <div class="label">Organization: <s:property value="organization.name"/></div>--%>

<%--                                <div class="info"></div>--%>

<%--                            </div>--%>
<%--                            <s:set var="lastOrg" value="#current"/>--%>

<%--                        </s:if>--%>

<%--                        <div title="<s:property value="name"/>"--%>
<%--                             onclick=" javascript:--%>
<%--                                     importTransform(<s:property value="uploadId"/>,<s:property value="dbID"/>,<s:property value="organizationId"/>);--%>
<%--                                     "--%>
<%--                             class="items navigable">--%>

<%--                            <div class="label" style="width:80%">--%>
<%--                                <s:property value="name"/> <font style="font-size:0.9em;margin-left:5px;color:grey;">(<s:property value="targetSchema"/>)</font></div>--%>

<%--                            <s:if test="xsl==true"><span style="color:#a00">XSL</span></s:if>--%>
<%--                            <div class="info">--%>
<%--                                <s:if test="isLocked(user, sessionId)">--%>
<%--                                    <img src="images/locked.png" title="locked mappings" style="top:4px;position:relative;max-width:18px;max-height:18px;padding-right:4px;">--%>
<%--                                </s:if>--%>
<%--                                <s:if test="isShared()">--%>
<%--                                    <img src="images/shared.png" title="shared mappings" style="top:4px;position:relative;max-width:18px;max-height:18px;">--%>
<%--                                </s:if>--%>
<%--                            </div>--%>
<%--                            <div class="tail"></div>--%>
<%--                        </div>--%>
<%--                    </s:iterator>--%>

<%--                </div>--%>
<%--            </s:if>--%>
            <div style="width:100%;height:30px"/>
            <div class="enrichments_pagination"></div>
            <div id="enrichmentPanel">
                <script>ajaxEnrichPanel(0,${organizationId},${user.dbID}, ${uploadId});</script>
            </div>

            <div class="enrichments_pagination"></div>


        </div>
    </div>
    <script type="text/javascript">
        $(document).ready(function() {
            /* use org id of import to show appropriate mappings*/
            <%if(request.getParameter("orgId")!=null){%>
            mapping_oid=<%=request.getParameter("orgId")%>;
            $('#filterenrichorg').val(mapping_oid);
            <%}%>
        });
    </script>

</s:elseif>


