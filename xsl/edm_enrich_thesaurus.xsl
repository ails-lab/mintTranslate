<?xml version="1.0"?>
<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
   xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
    xmlns:skos="http://www.w3.org/2004/02/skos/core#" >

    <xsl:output indent="yes" method="xml" encoding="utf-8" omit-xml-declaration="yes"/>

    <!-- find thesaurus refs and add them to the EDM output
         Copy everything, but after copying ProvidedCHO, insert the skos:Concepts
         -->

	<xsl:param name="thesaurusRdf" select="'euscreen_thesaurus.rdf'" />
	<xsl:param name="thesaurusPattern" select="'http://thesaurus.euscreen.eu/'" />
  
    <!-- template to copy elements -->
    <xsl:template match="@*|node()" mode="copySkos" >
        <xsl:if test="not( local-name()='scopeNote' or local-name()='type' )">
            <xsl:choose>
                <xsl:when test="local-name()='Description'">
                  <skos:Concept>
                        <xsl:attribute name="rdf:about">
                            <xsl:value-of select="@rdf:about"/>
                        </xsl:attribute>
                         <xsl:apply-templates select="node()" mode="copySkos"/>
                    </skos:Concept> 
                </xsl:when>
                <xsl:otherwise>
                    <xsl:copy> 
                         <xsl:apply-templates select="@*|node()" mode="copySkos"/>
                    </xsl:copy>
                </xsl:otherwise>
            </xsl:choose>
        </xsl:if>
    </xsl:template>
    
    <xsl:template name="insertConcept">
        <xsl:param name="url" />
          <xsl:apply-templates select="document($thesaurusRdf)/rdf:RDF/rdf:Description[@rdf:about=$url]" mode="copySkos"/>
    </xsl:template>

    <xsl:template match="*[local-name()='ProvidedCHO']">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
        <!-- Loop over every rdf:resource in the ProvidedCHO that starts with http://thesaurus.euscreen... -->
        <xsl:for-each select="//@*[ local-name()='resource' and starts-with( ., $thesaurusPattern )]">
            <xsl:call-template name="insertConcept">
                <xsl:with-param name="url" select="." />   
            </xsl:call-template>
        </xsl:for-each>     
    </xsl:template>

    <xsl:template match="@*|node()">
        <xsl:copy>
            <xsl:apply-templates select="@*|node()"/>
        </xsl:copy>
    </xsl:template>
</xsl:stylesheet>
