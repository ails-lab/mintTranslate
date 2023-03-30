<?xml version="1.0" encoding="UTF-8" ?>
    <xsl:transform xmlns:xsl="http://www.w3.org/1999/XSL/Transform" 
	     xmlns:local="urn://local/"
      exclude-result-prefixes="local"
     version="2.0">
    
    	<!-- xmlns:xs="http://www.w3.org/2001/XMLSchema" -->
	    
        <xsl:output omit-xml-declaration="no" indent="yes" encoding="UTF-8" />
    
	    <xsl:variable name="csv" >
    	<!--  insert csv/row/col style xml -->
        	<local:csv />
    	</xsl:variable>

        <local:matchValue />
        	<!-- xsl:variable name="matchValue" select="//edm:ProvidedCHO/dc:identifier" / -->

        <!--  The row[ xxx ] part needs to be adapted to with $matchValue to
        	  extract the relevant rows from the csv  -->

        <local:insertRows />	            
        	<!--  xsl:variable name="inserts" select="$csv/csv/row[matches( col[1],  concat( '_',$matchValue,'$'))]" / -->

	    <!--  This variable should be set for the insert point, no leading / -->	    
	    <local:parentPathString />
  			<!--   xsl:variable name="parentPathString" select="'rdf:RDF/edm:ProvidedCHO'" / -->

		<!-- A list of the elements that we need to appear after the insert point 
			 You need to set the names variable to array of strings with prefixes -->
		<local:beforeElements>
			<xsl:variable name="names" select="()" />
			<!-- xsl:variable name="names" select="('edm:isRepresentationOf', 'edm:isSimilarTo', 'edm:isSuccessorOf' , 'edm:realizes', 'edm:type')" /-->
		</local:beforeElements>
   
		<!--  Code to make an insert from a row/col ... this is generated from java -->
		<xsl:template name="createInsert" >
			<xsl:param name="row"/>
			<!--  insert code for enrichment, dependent on $row -->
			<local:insertXml/>
		</xsl:template>

			
      	<xsl:template match="/">
        	<!--  use this for an exact insert enrichment -->
			<xsl:call-template name="exactInserts" />     
        </xsl:template>
    
		<!--  This template should work as is, after the configs from further above -->
        <xsl:template name="exactInserts">          
		  
        	<xsl:variable name="parentPath" select="for $x in tokenize( $parentPathString,'/' ) return resolve-QName( $x, ./node() )" />
  			<xsl:variable name="beforeNames" select="for $x in  $names return resolve-QName( $x, ./node() )" /> 
	
			<xsl:apply-templates select="." mode="exactInserts" >
				<xsl:with-param name="rows" select="$inserts" />
				<xsl:with-param name="parentPath" select="$parentPath" />
				<xsl:with-param name="beforeNames" select="$beforeNames" />
			</xsl:apply-templates>

        </xsl:template>

		<!-- Check the name of given node against the QName list and return true() or false() -->
		<xsl:function name="local:elementNameInQnameList" >
			<xsl:param name="node" />
			<xsl:param name="names" />
			<xsl:variable name="nameMatches" select="for $x in $names return if ( $x = node-name($node)) then true() else ()" />

			<xsl:sequence select="not( empty( $nameMatches))" />
		</xsl:function>
		
		<!--  this template should work always ... inserting multiple rows that matched somehow -->
		<xsl:template mode="exactInserts" match="*">
			<xsl:param name="rows" />
			<xsl:param name="parentPath" />
			<xsl:param name="beforeNames" />
			
			
			<xsl:choose>
				<xsl:when test="empty( $rows )">
					<xsl:copy-of select="." /> 
				</xsl:when>
				<xsl:otherwise>
					<xsl:variable name="insertNodes">
						<xsl:call-template name="createInsert">
							<xsl:with-param name="row" select="$rows[1]" />
						</xsl:call-template>	
					</xsl:variable>
					<xsl:variable name="newDom">
						<xsl:call-template name="copyWithUnorderedInsert" >
			              	<xsl:with-param name="parentPath" select="$parentPath" />
            			  	<xsl:with-param name="insertNodes" select="$insertNodes" />
              				<xsl:with-param name="nodes" select="." />
		                    <xsl:with-param name="beforeNames" select="$beforeNames" />
						</xsl:call-template>
					</xsl:variable>
					<xsl:apply-templates mode="exactInserts" select="$newDom">
						<xsl:with-param name="rows" select="$rows[position()>1]" />
						<xsl:with-param name="parentPath" select="$parentPath" />
	                    <xsl:with-param name="beforeNames" select="$beforeNames" />
					
					</xsl:apply-templates>
				</xsl:otherwise>
			</xsl:choose> 
		</xsl:template>

        
        <!-- any of the nodes a parent of insertPath, then copy with unorderd insert in that one -->
          <xsl:template name="copyWithUnorderedInsert" >
          <xsl:param name="parentPath"/>
          <xsl:param name="insertNodes"/>
          <xsl:param name="nodes"/>
          <xsl:param name="beforeNames" />
          
          <xsl:choose>
            <xsl:when test="empty( $parentPath )">
			<!--  we are at the insert level, no parents left -->

              <xsl:choose>
              <!--  if there are before names, insert at the end, or before any of the element names in the beforeNames list -->
              <!--  if there are no before names, insert at the beginning -->
              	<xsl:when test="empty( $beforeNames )">
              		<!--  unordered insert at the beginning -->
              		<xsl:sequence select="$insertNodes"/>
              		<xsl:for-each select="$nodes" >
                		<xsl:copy-of select="." />
              		</xsl:for-each>
              	</xsl:when>

              	<!--  when $nodes empty just insert it -->
              	<!--  The parent has no children or we iterated over all of them -->
              	<xsl:when test="empty( $nodes )" >
              		<xsl:sequence select="$insertNodes"/>              		
              	</xsl:when>
              	
				<xsl:when test="local:elementNameInQnameList( $nodes[1], $beforeNames )" >
	              	<!--  if name of first node is in beforeNames, insert here -->
              		<xsl:sequence select="$insertNodes"/>
              		<xsl:for-each select="$nodes" >
                		<xsl:copy-of select="." />
              		</xsl:for-each>
              	</xsl:when>
              	<xsl:otherwise>
                   <xsl:copy-of select="$nodes[1]" />
	                <xsl:call-template name="copyWithUnorderedInsert">
	                    <xsl:with-param name="parentPath" select="$parentPath" />
	                    <xsl:with-param name="insertNodes" select="$insertNodes" />
	                    <xsl:with-param name="nodes" select="$nodes[position()>1]"/>
	                    <xsl:with-param name="beforeNames" select="$beforeNames" />
	                </xsl:call-template>	              		 
              	</xsl:otherwise>
              </xsl:choose> 
            </xsl:when>
            <xsl:when test="empty($nodes)" >
              <xsl:value-of select="$parentPath" />
              !!! This should not happen !!!
              <xsl:sequence select="$insertNodes" />
            </xsl:when>
            <xsl:when test="node-name($nodes[1])=$parentPath[1]" >
              <xsl:for-each select="$nodes[1]">
	              <xsl:copy>
	                <xsl:call-template name="copyWithUnorderedInsert">
	                    <xsl:with-param name="parentPath" select="$parentPath[position()>1]" />
	                    <xsl:with-param name="insertNodes" select="$insertNodes" />
	                    <xsl:with-param name="nodes" select="@*|node()"/>
	                    <xsl:with-param name="beforeNames" select="$beforeNames" />                    
	                </xsl:call-template>                  
	              </xsl:copy>
              </xsl:for-each>
              <xsl:for-each select="$nodes[position()>1]" >
                <xsl:copy-of select="." />
              </xsl:for-each>
            </xsl:when>
            <xsl:otherwise>
                <xsl:copy-of select="$nodes[1]" />
                <xsl:call-template name="copyWithUnorderedInsert">
                    <xsl:with-param name="parentPath" select="$parentPath" />
                    <xsl:with-param name="insertNodes" select="$insertNodes" />
                    <xsl:with-param name="nodes" select="$nodes[position()>1]"/>
                    <xsl:with-param name="beforeNames" select="$beforeNames" />
                </xsl:call-template>
            </xsl:otherwise>
          </xsl:choose>  
        </xsl:template>		
    </xsl:transform>
