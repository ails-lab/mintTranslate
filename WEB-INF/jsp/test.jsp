<%@ page import="gr.ntua.ivml.mint.persistent.Enrichment" %>
<%@ include file="_include.jsp"%>
<%@ page language="java" errorPage="error.jsp"%>
<%--
  Created by IntelliJ IDEA.
  User: goji
  Date: 1/25/21
  Time: 5:44 PM
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <title>Title</title>
</head>
<body>
<s:iterator value="accessibleEnrichments">
    <h1><s:property value="name"/></h1>
</s:iterator>
</body>
</html>
