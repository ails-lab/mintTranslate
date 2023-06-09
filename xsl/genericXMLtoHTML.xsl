<?xml version="1.0"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform">

    <xsl:output indent="yes" method="html" encoding="utf-8" omit-xml-declaration="yes"/>

    <!-- Stylesheet to remove all namespaces from a document -->
    <!-- NOTE: this will lead to attribute name clash, if an element contains
        two attributes with same local name but different namespace prefix -->
    <!-- Nodes that cannot have a namespace are copied as such -->



    <!-- template to copy elements -->
    <xsl:template match="/">
    <html>
    	<head>
    		<title>Record</title>
    	</head>
    	<body> 
        	<xsl:apply-templates select="/*"/>        
	</body>
	</html>
    </xsl:template>
    
    <xsl:template match="*">
    		<div style="margin-left: 30px;margin-top: 10px;">
			<b><xsl:value-of select="local-name()"/></b>    			
       		<xsl:for-each select="@*">
			  <br/>
			  <i>@<xsl:value-of select="concat(name(), '=&quot;', ., '&quot;')" /></i>
            </xsl:for-each>
            <br/>
        	<xsl:value-of select="text()"/>
 	    	<xsl:apply-templates select="*"/>    
        	</div>
    </xsl:template>
</xsl:stylesheet>
