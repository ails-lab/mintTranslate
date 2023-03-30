
<%@ include file="_include.jsp"%>
<%@ page language="java" errorPage="error.jsp"%>
<%@page pageEncoding="UTF-8"%>
<%@page import="gr.ntua.ivml.mint.persistent.Organization"%>
<%@page import="gr.ntua.ivml.mint.persistent.XmlSchema"%>
<%@page import="java.util.List"%>


<script type="text/javascript" src="js/oaiRequest.js"></script>

<script type="text/javascript">

    function createUploader(){
        var uploader = new qq.FileUploader({
            element: document.getElementById('uploadFile'),
            action: 'AjaxFileReader.action',
            debug: true
        });
    }



    $(function() {

        $('#Import_mthhttpupload').click(function() {
            $('input[id^="Import_"]').attr("disabled", true);
            $('input[id^="Import_mth"]').attr( "disabled",false);
            $("#Import_httpup").attr( "disabled",false );
            $("#Import_directSchema").attr( "disabled",false);
            $("#isDirect").attr( "disabled",false);
            $(":button").attr( "disabled",false );
            $(':input[id^="Import_csv"]').attr( "disabled",false );$("#Import_isCsv:input").attr( "disabled",false );


        })
        $('#Import_mthurlupload').click(function() {
            $('input[id^="Import_"]').attr("disabled", true);
            $(":button").attr( "disabled",false );
            $("#Import_directSchema").attr( "disabled",false);
            $("#isDirect").attr( "disabled",false);
            $("#Import_uploaderOrg").attr( "disabled",false);
            $('input[id^="Import_mth"]').attr( "disabled",false);
            $("input[id^='Import_uploadUrl']").attr( "disabled",false);

        })

        $('#Import_mthftpupload').click(function() {
            $('input[id^="Import_"]').attr("disabled", true);
            $("#Import_directSchema").attr( "disabled",false );$("#isDirect").attr( "disabled",false);
            $("#Import_uploaderOrg").attr( "disabled",false);
            $('input[id^="Import_mth"]').attr( "disabled",false);
            $(":button").attr( "disabled",false );
            $("#Import_flist").attr( "disabled",false);
        })

        $('#Import_mthSuperUser').click(function() {
            $('input[id^="Import_"]').attr("disabled", true); $("#Import_directSchema").attr( "disabled",false);$("#isDirect").attr( "disabled",false);
            $("#Import_uploaderOrg").attr( "disabled",false);
            $('input[id^="Import_mth"]').attr( "disabled",false);
            $(":button").attr( "disabled",false );
            $("#Import_serverFilename").attr( "disabled",false);

        })

        $("#submit_import").click(function() {
            importEnrichment();

        })

        if($('ul.qq-upload-list').html()==null)

        {createUploader();}
    });


</script>

<div class="panel-body">

    <div class="block-nav">
        <div class="summary">
            <div class="label">Import</div>
            <div id="info">
                <s:if test="hasActionErrors()">
                    <s:iterator value="actionErrors">
                        <font style="color:red;"><s:property escapeHtml="false" /> </font>
                    </s:iterator>
                </s:if>
            </div>
        </div>


        <s:form name="impenform" action="ImportEnrichment" cssClass="athform" theme="mytheme"
                enctype="multipart/form-data" method="POST">

        <div class="fitem">
            <div id="fileoption" >
                <s:radio name="mth" list="%{#{'httpupload':'Local upload'}}" theme="simple" label="Local Upload"
                         cssStyle="float:left;" cssClass="checks"/>

                First upload the file:

                <div id="uploadFile">
                    <noscript>
                        <p>Please enable JavaScript to use file uploader.</p>
                    </noscript>
                </div>
                <input type="hidden" id="upfile" name="upfile" value='<s:property value="upfile"/>' />
                <input type="hidden" id="httpup" name="httpup" value='<s:property value="httpup"/>' />

                <s:if test="(httpup!=null && httpup.length()>0 && !httpup.equalsIgnoreCase('undefined'))">
                    <div class="qq-uploader">
                        <ul class="qq-upload-list"><li class="qq-upload-success"><s:property value="httpup"/></li></ul>
                    </div>
                </s:if>
            </div>


            <div><i>(Only .csv files allowed)</i></div>
        </div>


<%--        <div class="fitem">--%>
<%--            <s:radio name="mth"--%>
<%--                     list="%{#{'urlupload':'Remote FTP/HTTP Upload'}}"  cssStyle="float:left;"  cssClass="checks"/>--%>
<%--            <s:textfield name="uploadUrl" size="60px;" disabled="true"  /> <font--%>
<%--                style="font-size: 10px;"><br/></font>--%>
<%--        </div>--%>

        <s:if test="%{user.accessibleOrganizations.size>1}">
        <div class="fitem"><label>Upload for Organization</label><s:select name="uploaderOrg"
                                                                           headerKey="0" headerValue="-- Which Organization --"
                                                                           list="user.accessibleOrganizations" listKey="dbID" listValue="name" value="user.organization.dbID"
                                                                           required="true"/> <br/><font style="font-size: 0.9em;"><i>Parent
            organization upload support</i> </font>
            </s:if>
            <s:else>
                <!--  only one organization is accessible -->
                <div class="fitem">
                    <label> Upload for Organization: </label> <s:property value="user.accessibleOrganizations.get(0).name" />
                </div>
            </s:else>
            <% List<XmlSchema> l=(List<XmlSchema>)request.getAttribute("xmlSchemas");
            %>

            <p align="left">
                <a class="navigable focus k-focus" id="submit_import">
                    <span>Submit</span></a>
                <a class="navigable focus k-focus"  onclick="this.blur(); document.impenform.reset();"><span>Reset</span></a>
            </p>


            </s:form>
            <script type="text/javascript">

                <%if(request.getParameter("mth")!=null){%>
                var mthr=document.getElementsByName('mth');
                for (var i=0; i<mthr.length; i++)  {
                    if (mthr[i].checked)  {

                        mthr[i].disabled=false;
                        mthr[i].click();
                    }
                }
                <%}%>
            </script>
        </div>
    </div>


