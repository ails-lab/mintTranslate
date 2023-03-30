<?xml version="1.0"?>
<xsl:stylesheet xmlns:xsl="http://www.w3.org/1999/XSL/Transform" xmlns:crm="http://www.cidoc-crm.org/rdfs/cidoc_crm_v5.0.2_english_label.rdfs#" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:edm="http://www.europeana.eu/schemas/edm/" xmlns:foaf="http://xmlns.com/foaf/0.1/" xmlns:lido="http://www.lido-schema.org" xmlns:ore="http://www.openarchives.org/ore/terms/" xmlns:owl="http://www.w3.org/2002/07/owl#" xmlns:rdaGr2="http://rdvocab.info/ElementsGr2/" xmlns:gml="http://www.opengis.net/gml" xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#" xmlns:skos="http://www.w3.org/2004/02/skos/core#" xmlns:wgs84_pos="http://www.w3.org/2003/01/geo/wgs84_pos#" xmlns:svcs="http://rdfs.org/sioc/services#" xmlns:xalan="http://xml.apache.org/xalan" exclude-result-prefixes="lido xml" version="2.0">
  <xsl:output method="xml" encoding="UTF-8" standalone="yes" indent="yes" />
  
	<!-- lidoEdm-20200524-museu.xsl -->
<!--
        xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
        xx  XSL Transform to convert LIDO XML data, according to http://www.lido-schema.org/schema/v1.0/lido-v1.0.xsd, 
        xx    into EDM RDF/XML, according to http://www.europeana.eu/schemas/edm/EDM.xsd (EDM v5.2.8, 2017-10-06)
        xx
        xx  Originally prepared for Linked Heritage (http://www.linkedheritage.org/) / Athena Plus (http://www.athenaplus.eu/) by 
		xx	Nikolaos Simou, National Technical University of Athens - NTUA (nsimou@image.ntua.gr)
		xx	Regine Stein, Bildarchiv Foto Marburg, Philipps-Universitaet Marburg - UNIMAR (r.stein@fotomarburg.de)
		xx
		xx  Current version: 2020-05-24 for Museu (http://www.museuhub.eu/)
		xx  Regine Stein, Goettingen State and University Library, Georg-August-Universitaet Goettingen - UGOE (regine.stein@sub.uni-goettingen.de)
		xx
		xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
		xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
-->
  
 <xsl:variable name="var0">
    <item>TEXT</item>
    <item>VIDEO</item>
    <item>IMAGE</item>
    <item>SOUND</item>
    <item>3D</item>
  </xsl:variable>
  
  
    <!-- Specify edm:provider -->
 
 <xsl:param name="edm_provider" select="'Museu'" />
  <!-- Specify edm:providedCHO baseURL -->
  <xsl:param name="edm_providedCHO" select="'http://mint-projects.image.ntua.gr/Museu/ProvidedCHO/'" />
  <!-- Specify ore:Aggregation baseURL -->
  <xsl:param name="ore_Aggregation" select="'http://mint-projects.image.ntua.gr/Museu/Aggregation/'" />
  
  <xsl:function name="lido:langSelect">
    <xsl:param name="metadataLang" />
    <xsl:param name="localLang" />
        <xsl:if test="( string-length($metadataLang) &lt; 4 and string-length($metadataLang) &gt; 0) or             (string-length($localLang) &lt; 4 and string-length($localLang) &gt; 0)">
			<xsl:choose>
  			<xsl:when test="(string-length($localLang) &lt; 4 and string-length($localLang) &gt; 0)">
  					<xsl:value-of select="$localLang" />
  				</xsl:when>
  				<xsl:when test="( string-length($metadataLang) &lt; 4 and string-length($metadataLang) &gt; 0)">
  					<xsl:value-of select="$metadataLang" />
  				</xsl:when>
  				<xsl:otherwise />
  			</xsl:choose>
    	</xsl:if>
  </xsl:function>
 
  <xsl:template match="/"> 
     <rdf:RDF>
 
  <xsl:for-each select="/lido:lidoWrap/lido:lido | lido:lido">
  
    <xsl:variable name="descLang">
    	<xsl:for-each select="lido:descriptiveMetadata/@xml:lang">
			<xsl:value-of select="." />
		</xsl:for-each>
    </xsl:variable>
    
     <xsl:variable name="adminLang">
    	<xsl:for-each select="lido:administrativeMetadata/@xml:lang">
			<xsl:value-of select="." />
		</xsl:for-each>
    </xsl:variable>
    
    <xsl:variable name="dataProvider">
    	<xsl:choose>
			<xsl:when test="lido:administrativeMetadata/lido:recordWrap/lido:recordSource[lido:type='europeana:dataProvider']">
				<xsl:for-each select="lido:administrativeMetadata/lido:recordWrap/lido:recordSource[lido:type='europeana:dataProvider']/lido:legalBodyName/lido:appellationValue[string-length(text()) &gt; 0]">
				  <xsl:if test="position() = 1">
					<edm:dataProvider>
					  <xsl:value-of select="." />
					</edm:dataProvider>
				  </xsl:if>
				</xsl:for-each>
			</xsl:when>
			<xsl:otherwise>
				<xsl:for-each select="lido:administrativeMetadata/lido:recordWrap/lido:recordSource/lido:legalBodyName/lido:appellationValue[string-length(text()) &gt; 0]">
				  <xsl:if test="position() = 1">
					<edm:dataProvider>
					  <xsl:value-of select="." />
					</edm:dataProvider>
				  </xsl:if>
				</xsl:for-each>
			</xsl:otherwise>
		</xsl:choose>
    </xsl:variable>


      <edm:ProvidedCHO>
        <xsl:attribute name="rdf:about">
        	<xsl:for-each select="lido:administrativeMetadata/lido:recordWrap/lido:recordID[string-length(text()) &gt; 0]">
            <xsl:if test="position() = 1">
              <xsl:value-of select="concat($edm_providedCHO, $dataProvider,'/',.)" />
            </xsl:if>
          </xsl:for-each>
        </xsl:attribute>
        
        <!-- dc:contributor : lido:eventActor with lido:eventType NOT production or creation or designing or publication -->
        <xsl:for-each select="lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event">
        <xsl:if test="not((lido:eventType/lido:conceptID = 'http://terminology.lido-schema.org/lido00007') or (lido:eventType/lido:conceptID = 'http://terminology.lido-schema.org/lido00012') or (lido:eventType/lido:conceptID = 'http://terminology.lido-schema.org/lido00224') or (lido:eventType/lido:conceptID = 'http://terminology.lido-schema.org/lido00228'))">

        	<xsl:for-each select="lido:eventActor">
        		<xsl:choose>
        		<!-- dc:contributor : lido:eventActor with assigned preferred URI and additional information (multiple URIs or vital dates) for contextual class edm:Agent -->
        			<xsl:when test="(count(lido:actorInRole/lido:actor/lido:actorID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and (lido:displayActorInRole or lido:actorInRole/lido:actor/lido:nameActorSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]) and ((lido:actorInRole/lido:actor/lido:actorID[starts-with(@lido:pref, 'alternat')][starts-with(., 'http://') or starts-with(., 'https://')]) or (lido:actorInRole/lido:actor/lido:vitalDatesActor[lido:earliestDate[string-length(text()) &gt; 0] or lido:latestDate[string-length(text()) &gt; 0]]))">
       						<dc:contributor>
       							<xsl:attribute name="rdf:resource">
       								<xsl:value-of select="lido:actorInRole/lido:actor/lido:actorID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]" />
       							</xsl:attribute>
       						</dc:contributor>
        			</xsl:when>
        			<xsl:otherwise>      		
        				
        		<!-- dc:contributor : lido:eventActor with URI from a Europeana dereferenceable vocab, no additional information -->
        		<xsl:if test="(count(lido:actorInRole/lido:actor/lido:actorID[starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and not(lido:displayActorInRole or lido:actorInRole/lido:actor/lido:nameActorSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)])">
                	<dc:contributor>
                    	<xsl:attribute name="rdf:resource">
    						<xsl:value-of select="lido:actorInRole/lido:actor/lido:actorID[starts-with(., 'http://') or starts-with(., 'https://')]" />
						</xsl:attribute>
					</dc:contributor>
				</xsl:if>
       
        		<!-- dc:contributor Literal : lido:eventActor with displayActorInRole -->
        		<xsl:if test="lido:displayActorInRole">
        			<xsl:for-each select="lido:displayActorInRole[string-length(text()) &gt; 0]">
        				<!-- include all labels for language variants -->
        					<dc:contributor>
        						<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
        							<xsl:attribute name="xml:lang">
        								<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
        							</xsl:attribute>
        						</xsl:if>
        						<xsl:value-of select="." />
        					</dc:contributor>
        			</xsl:for-each>
        		</xsl:if>

        		<!-- dc:contributor Literal : lido:eventActor without displayActorInRole but nameActorSet -->
        		<xsl:if test="not(lido:displayActorInRole) and lido:actorInRole/lido:actor/lido:nameActorSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]">
        			<xsl:for-each select="lido:actorInRole/lido:actor/lido:nameActorSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)][string-length(text()) &gt; 0]">
        				<!-- include all labels for language variants -->
        					<dc:contributor>
        						<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
        							<xsl:attribute name="xml:lang">
        								<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
        							</xsl:attribute>
        						</xsl:if>
        						<xsl:value-of select="." />
        					</dc:contributor>
        			</xsl:for-each>
        		</xsl:if>	
        				
        			</xsl:otherwise>
        		</xsl:choose>
        	</xsl:for-each>
        </xsl:if>
        </xsl:for-each>
         
      	<!-- dc:contributor : lido:culture from any lido:eventSet -->
      	<xsl:for-each select="lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event/lido:culture">
      		<!-- dc:contributor Resource with URI from a Europeana dereferenceable vocab -->
        <xsl:if test="(count(lido:conceptID[starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and not(lido:term)">
            <dc:contributor>
				<xsl:attribute name="rdf:resource">
					<xsl:value-of select="lido:conceptID[starts-with(., 'http://') or starts-with(., 'https://')]" />
				</xsl:attribute>
            </dc:contributor>
		</xsl:if>
		<!-- dc:contributor Resource-->
		
        <!-- dc:contributor Literal-->
        <xsl:if test="not(lido:conceptID)"> 
        	<xsl:for-each select="lido:term[@lido:pref='preferred' or (not(@lido:pref) and not(@lido:addedSearchTerm='yes'))][string-length(text()) &gt; 0]">
				<dc:contributor>
                <xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
        				<xsl:attribute name="xml:lang">
        					<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
        				</xsl:attribute>
    				</xsl:if>			
					<xsl:value-of select="." />
				</dc:contributor>				
			</xsl:for-each>
		</xsl:if>
        <!-- dc:contributor : lido:culture -->
      	</xsl:for-each>
        
      	<!-- dc:creator : lido:eventActor with lido:eventType = production or creation or designing-->
      	<xsl:for-each select="lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event">
      		<xsl:if test="(lido:eventType/lido:conceptID = 'http://terminology.lido-schema.org/lido00007') or (lido:eventType/lido:conceptID = 'http://terminology.lido-schema.org/lido00012') or (lido:eventType/lido:conceptID = 'http://terminology.lido-schema.org/lido00224')">

      			<xsl:for-each select="lido:eventActor">
      				<xsl:choose>
      					<!-- dc:creator : lido:eventActor with assigned preferred URI and additional information (multiple URIs or vital dates) for contextual class edm:Agent -->
      					<xsl:when test="(count(lido:actorInRole/lido:actor/lido:actorID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and (lido:displayActorInRole or lido:actorInRole/lido:actor/lido:nameActorSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]) and ((lido:actorInRole/lido:actor/lido:actorID[starts-with(@lido:pref, 'alternat')][starts-with(., 'http://') or starts-with(., 'https://')]) or (lido:actorInRole/lido:actor/lido:vitalDatesActor[lido:earliestDate[string-length(text()) &gt; 0] or lido:latestDate[string-length(text()) &gt; 0]]))">
      						<dc:creator>
      							<xsl:attribute name="rdf:resource">
      								<xsl:value-of select="lido:actorInRole/lido:actor/lido:actorID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]" />
      							</xsl:attribute>
      						</dc:creator>
      					</xsl:when>
      					<xsl:otherwise>      		
      						
      						<!-- dc:creator : lido:eventActor with URI from a Europeana dereferenceable vocab, no additional information -->
      						<xsl:if test="(count(lido:actorInRole/lido:actor/lido:actorID[starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and not(lido:displayActorInRole or lido:actorInRole/lido:actor/lido:nameActorSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)])">
      							<dc:creator>
      								<xsl:attribute name="rdf:resource">
      									<xsl:value-of select="lido:actorInRole/lido:actor/lido:actorID[starts-with(., 'http://') or starts-with(., 'https://')]" />
      								</xsl:attribute>
      							</dc:creator>
      						</xsl:if>
      						
      						<!-- dc:creator Literal : lido:eventActor with displayActorInRole -->
      						<xsl:if test="lido:displayActorInRole">
      							<xsl:for-each select="lido:displayActorInRole[string-length(text()) &gt; 0]">
      								<!-- include all labels for language variants -->
      									<dc:creator>
      										<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
      											<xsl:attribute name="xml:lang">
      												<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
      											</xsl:attribute>
      										</xsl:if>
      										<xsl:value-of select="." />
      									</dc:creator>
      							</xsl:for-each>
      						</xsl:if>
      						
      						<!-- dc:creator Literal : lido:eventActor without displayActorInRole but nameActorSet -->
      						<xsl:if test="not(lido:displayActorInRole) and lido:actorInRole/lido:actor/lido:nameActorSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]">
      							<xsl:for-each select="lido:actorInRole/lido:actor/lido:nameActorSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)][string-length(text()) &gt; 0]">
      								<!-- include all labels for language variants -->
      									<dc:creator>
      										<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
      											<xsl:attribute name="xml:lang">
      												<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
      											</xsl:attribute>
      										</xsl:if>
      										<xsl:value-of select="." />
      									</dc:creator>
      							</xsl:for-each>
      						</xsl:if>	
      						
      					</xsl:otherwise>
      				</xsl:choose>
      			</xsl:for-each>
      		</xsl:if>
        </xsl:for-each>
	    
	    <!-- dc:date -->
	    <!-- dc:date : lido:eventDate with lido:eventType NOT production or creation or designing -->
        <xsl:for-each select="lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event">
        <xsl:if test="not((lido:eventType/lido:conceptID = 'http://terminology.lido-schema.org/lido00007') or (lido:eventType/lido:conceptID = 'http://terminology.lido-schema.org/lido00012') or (lido:eventType/lido:conceptID = 'http://terminology.lido-schema.org/lido00224'))">
        <xsl:for-each select="lido:eventDate">
        	<xsl:if test="lido:date/lido:earliestDate[string-length(text()) &gt; 0] | lido:displayDate[string-length(text()) &gt; 0]">
        	<dc:date>
				<xsl:choose>
					<xsl:when test="lido:date/lido:earliestDate = lido:date/lido:latestDate">
						<xsl:attribute name="xml:lang">
							<xsl:value-of select="lido:langSelect($descLang,lido:date/lido:earliestDate[1]/@xml:lang)" />
						</xsl:attribute>
						<xsl:value-of select="lido:date/lido:earliestDate" />
					</xsl:when>
					<xsl:when test="lido:date/lido:earliestDate[string-length(text()) &gt; 0]">
						<xsl:attribute name="xml:lang">
							<xsl:value-of select="lido:langSelect($descLang,lido:date/lido:earliestDate[1]/@xml:lang)" />
						</xsl:attribute>
						<xsl:value-of select="concat(lido:date/lido:earliestDate, '/', lido:date/lido:latestDate)" />
					</xsl:when>
					<xsl:otherwise>
						<xsl:if test="string-length( lido:langSelect($descLang,lido:displayDate[1]/@xml:lang)) &gt; 0">
							<xsl:attribute name="xml:lang">
								<xsl:value-of select="lido:langSelect($descLang,lido:displayDate[1]/@xml:lang)" />
							</xsl:attribute>
						</xsl:if>	
						<xsl:value-of select="lido:displayDate" />
					</xsl:otherwise>
				</xsl:choose>
			</dc:date>
		</xsl:if>
        </xsl:for-each>
        </xsl:if>
        </xsl:for-each>
        <!-- dc:date -->  
		
		<!-- dc:description : lido:objectDescriptionSet -->   
    	<xsl:for-each select="lido:descriptiveMetadata/lido:objectIdentificationWrap/lido:objectDescriptionWrap/lido:objectDescriptionSet[lido:descriptiveNoteValue/string-length(.)&gt;0]">
		<xsl:choose>
			<xsl:when test="lido:descriptiveNoteID or lido:sourceDescriptiveNote">
			<dc:description>
				<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
        			<xsl:attribute name="xml:lang">
        				<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
        			</xsl:attribute>
    			</xsl:if>
				<xsl:if test="@lido:type">
					<xsl:value-of select="concat(@lido:type, ': ')" />
				</xsl:if>
				<xsl:for-each select="lido:descriptiveNoteValue">
					<xsl:value-of select="concat(., ' ')" />
				</xsl:for-each>
				<xsl:if test="string-length(lido:sourceDescriptiveNote[1])&gt;0">
					<xsl:value-of select="concat(' (', lido:sourceDescriptiveNote[1], ')')" />
				</xsl:if>
				<xsl:if test="string-length(lido:descriptiveNoteID[1])&gt;0">
					<xsl:value-of select="concat(' (', lido:descriptiveNoteID[1], ')')" />
				</xsl:if>
			</dc:description>
			</xsl:when>
			<xsl:otherwise>
				<xsl:for-each select="lido:descriptiveNoteValue[string-length(.)&gt;0]">
					<dc:description>
						<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
							<xsl:attribute name="xml:lang">
								<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
							</xsl:attribute>
						</xsl:if>
						<xsl:if test="../@lido:type">
							<xsl:value-of select="concat(../@lido:type, ': ')" />
						</xsl:if>
						<xsl:value-of select="normalize-space(text())" />
					</dc:description>
				</xsl:for-each>
			</xsl:otherwise>
		</xsl:choose>
		</xsl:for-each>
		<!-- dc:description -->   
		
      	<!-- dc:format : lido:eventMaterialsTech//lido:termMaterials NOT @lido:type=material: Update 2020-02-02 -->
      	<xsl:for-each select="lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event/lido:eventMaterialsTech/lido:materialsTech/lido:termMaterialsTech[not(lower-case(@lido:type)='material')]">
      		<xsl:choose>
      			<!-- with assigned preferred URI and additional information for contextual class skos:Concept -->
      			<xsl:when test="(count(lido:conceptID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and (lido:conceptID[starts-with(@lido:pref, 'alternat')][starts-with(., 'http://') or starts-with(., 'https://')]) and (lido:term[((@lido:pref='preferred') or (not(@lido:pref) and not(@lido:addedSearchTerm='yes')))])">
      				<dc:format>
      					<xsl:attribute name="rdf:resource">
      						<xsl:value-of select="lido:conceptID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]" />
      					</xsl:attribute>
      				</dc:format>
      			</xsl:when>
      			<xsl:otherwise>      		
      				<!-- with only URI from Europeana dereferenceable vocab -->
      				<xsl:if test="(count(lido:conceptID[starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and not(lido:term)">
      					<dc:format>
      						<xsl:attribute name="rdf:resource">
      							<xsl:value-of select="lido:conceptID[starts-with(., 'http://') or starts-with(., 'https://')]" />
      						</xsl:attribute>
      					</dc:format>
      				</xsl:if>
      				<!-- no dereferenceable URI, provide term as Literal -->
      				<xsl:for-each select="lido:term[@lido:pref='preferred' or (not(@lido:pref) and not(@lido:addedSearchTerm='yes'))][string-length(text()) &gt; 0]">
      					<dc:format>
      						<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
      							<xsl:attribute name="xml:lang">
      								<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
      							</xsl:attribute>
      						</xsl:if>			
      						<xsl:value-of select="." />
      					</dc:format>				
      				</xsl:for-each>
      			</xsl:otherwise>
      		</xsl:choose>
      	</xsl:for-each>
      	
      	<!-- dc:format : lido:eventMaterialsTech without controlled termMaterialsTech values -->
      	<xsl:for-each select="lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event/lido:eventMaterialsTech[not(lido:materialsTech/lido:termMaterialsTech)]">
	      	<xsl:for-each select="lido:displayMaterialsTech[string-length(text()) &gt; 0]">
	      		<dc:format>
	      			<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
	      				<xsl:attribute name="xml:lang">
	      					<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
	      				</xsl:attribute>
	      			</xsl:if>
	      			<xsl:value-of select="normalize-space(text())" />
	      		</dc:format>
	      	</xsl:for-each>
      	</xsl:for-each>


        <!-- dc:identifier : lido:objectPublishedID -->
      	<xsl:for-each select="lido:objectPublishedID[string-length(text()) &gt; 0]">
          <dc:identifier>
            <xsl:value-of select="." />
          </dc:identifier>
        </xsl:for-each>
        <!-- dc:identifier -->

       <!-- dc:identifier : lido:workID -->
      	<xsl:for-each select="lido:descriptiveMetadata/lido:objectIdentificationWrap/lido:repositoryWrap/lido:repositorySet[not(@lido:type='former')]/lido:workID[string-length(text()) &gt; 0]">
          <dc:identifier>
            <xsl:value-of select="." />
          </dc:identifier>
        </xsl:for-each>
        <!-- dc:identifier -->
        
        <!-- dc:identifier -->
      	<xsl:for-each select="lido:administrativeMetadata/lido:recordWrap/lido:recordID[string-length(text()) &gt; 0]">
        	<dc:identifier>
            	<xsl:value-of select="." />
          	</dc:identifier>
        </xsl:for-each>
        <!-- dc:identifier -->

		<!-- dc:language : lido:classification / @lido:type=language (MANDATORY with edm:type=TEXT) -->        
        <xsl:for-each select="lido:descriptiveMetadata/lido:objectClassificationWrap/lido:classificationWrap/lido:classification[@lido:type='language']/lido:term">
          <dc:language>
            <xsl:value-of select="." />
          </dc:language>
        </xsl:for-each>
		<!-- dc:language -->    
		
		<!-- dc:publisher : lido:eventActor with lido:eventType = publication -->
		<!-- dc:publisher Resource-->  
		<xsl:for-each select="lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event">
         <xsl:if test="(lido:eventType/lido:conceptID = 'http://terminology.lido-schema.org/lido00228')">     
            <xsl:for-each select="lido:eventActor">
			<xsl:if test="lido:actorInRole/lido:actor/lido:actorID[starts-with(., 'http://') or starts-with(., 'https://')]">
                <dc:publisher>
					<xsl:attribute name="rdf:resource">
					<xsl:for-each select="lido:actorInRole/lido:actor/lido:actorID[starts-with(., 'http://') or starts-with(., 'https://')]">
					<xsl:if test="position() = 1">
						<xsl:value-of select="." />
					</xsl:if>
					</xsl:for-each>
					</xsl:attribute>
                </dc:publisher>
			</xsl:if>
            </xsl:for-each>
        </xsl:if>
        </xsl:for-each>
		<!-- dc:publisher Resource-->  
		
		<!-- dc:publisher Literal-->  
        <xsl:for-each select="lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event">
        	<xsl:if test="(lido:eventType/lido:conceptID = 'http://terminology.lido-schema.org/lido00228')">
        	<xsl:if test="not(lido:eventActor/lido:actorInRole/lido:actor/lido:actorID)"> <!-- FILTER ACTOR NAME -->
        	    
        		<xsl:if test="lido:eventActor/lido:displayActorInRole[string-length(text()) &gt; 0]"> 
        		<xsl:for-each select="lido:eventActor/lido:displayActorInRole[string-length(text()) &gt; 0]">
        			<dc:publisher>
        				<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
        					<xsl:attribute name="xml:lang">
        						<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
        					</xsl:attribute>
    					</xsl:if>
    					<xsl:value-of select="." />
    				</dc:publisher>
    			</xsl:for-each>
    		</xsl:if>
    		
        	<xsl:if test="lido:eventActor/lido:actorInRole/lido:actor/lido:nameActorSet/lido:appellationValue"> 
        		<xsl:for-each select="lido:eventActor/lido:actorInRole/lido:actor/lido:nameActorSet/lido:appellationValue[string-length(text()) &gt; 0]">
        				<dc:publisher>
        					<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
        						<xsl:attribute name="xml:lang">
        							<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
        						</xsl:attribute>
    						</xsl:if>
    						<xsl:value-of select="." />
    					</dc:publisher>
    				</xsl:for-each>
    		</xsl:if>
        	</xsl:if>
       	 </xsl:if>
        </xsl:for-each>
		<!-- dc:publisher Literal-->  
		          
        <!-- dc:relation -->
		<xsl:for-each select="lido:descriptiveMetadata/lido:objectRelationWrap/lido:relatedWorksWrap/lido:relatedWorkSet">
            <xsl:if test="lido:relatedWorkRelType/lido:conceptID = 'http://purl.org/dc/elements/1.1/relation' and                (lido:relatedWork/lido:object/lido:objectWebResource or lido:relatedWork/lido:object/lido:objectID or                                 lido:relatedWork/lido:object/lido:objectNote)">
            	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectWebResource[string-length(text()) &gt; 0]">  
            <xsl:if test="starts-with(.,'http')">
                <dc:relation>
                <xsl:attribute name="rdf:resource">
        			<xsl:value-of select="." />
                </xsl:attribute>    
        		</dc:relation>
        	</xsl:if>
        	</xsl:for-each>
            	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectID[string-length(text()) &gt; 0]">
            	<dc:relation>
        			<xsl:value-of select="." />
        		</dc:relation>
        	</xsl:for-each>
            	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectNote[string-length(text()) &gt; 0]">
                <dc:relation>
        			<xsl:value-of select="." />
        		</dc:relation>
        	</xsl:for-each>
        	</xsl:if>
        </xsl:for-each>
        
        <!-- No LIDO property is mapped to dc:coverage of EDM -->
        
        <!-- dc:rights : lido:rightsWorkSet (rights for the CHO) -->
        <xsl:for-each select="lido:administrativeMetadata/lido:rightsWorkWrap/lido:rightsWorkSet">
    		  <xsl:choose>
    		  	<xsl:when test="lido:creditLine[string-length(text()) &gt; 0]">
                    <dc:rights>
                    	<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
                    		<xsl:attribute name="xml:lang">
                    			<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
                    		</xsl:attribute>
                    	</xsl:if>
                    	<xsl:value-of select="lido:creditLine" />
                    </dc:rights>
                </xsl:when>
    		  	<xsl:when test="lido:rightsHolder/lido:legalBodyName/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)][string-length(text()) &gt; 0]">
    		  		<xsl:for-each select="lido:rightsHolder/lido:legalBodyName/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)][string-length(text()) &gt; 0]">
				    <dc:rights>
				    	<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
				    		<xsl:attribute name="xml:lang">
				    			<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
				    		</xsl:attribute>
				    	</xsl:if>
				    	<xsl:value-of select="." />
					</dc:rights>
				</xsl:for-each>
				</xsl:when>
			</xsl:choose>
        </xsl:for-each>
        
        <!-- dc:source -->
        <xsl:for-each select="lido:descriptiveMetadata/lido:objectRelationWrap/lido:relatedWorksWrap/lido:relatedWorkSet">
         <xsl:if test="lido:relatedWorkRelType/lido:conceptID='http://purl.org/dc/elements/1.1/source' and (lido:relatedWork/lido:object/lido:objectWebResource or lido:relatedWork/lido:object/lido:objectID or lido:relatedWork/lido:object/lido:objectNote)">
         	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectWebResource[string-length(text()) &gt; 0]">  
            <xsl:if test="starts-with(.,'http')">
                <dc:source>
                <xsl:attribute name="rdf:resource">
        			<xsl:value-of select="." />
                </xsl:attribute>    
        		</dc:source>
        	</xsl:if>
        	</xsl:for-each>
         	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectID[string-length(text()) &gt; 0]">
            	<dc:source>
        			<xsl:value-of select="." />
        		</dc:source>
        	</xsl:for-each>
         	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectNote[string-length(text()) &gt; 0]">
                <dc:source>
        			<xsl:value-of select="." />
        		</dc:source>
        	</xsl:for-each>
        
          </xsl:if>
        </xsl:for-each>
        
      	<!-- dc:subject : lido:subjectConcept: Update 2020-02-02 -->
      	<xsl:for-each select="lido:descriptiveMetadata/lido:objectRelationWrap/lido:subjectWrap/lido:subjectSet/lido:subject/lido:subjectConcept">
      		<xsl:choose>
      			<!-- with assigned preferred URI and additional information for contextual class skos:Concept -->
      			<xsl:when test="(count(lido:conceptID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and (lido:conceptID[starts-with(@lido:pref, 'alternat')][starts-with(., 'http://') or starts-with(., 'https://')]) and (lido:term[((@lido:pref='preferred') or (not(@lido:pref) and not(@lido:addedSearchTerm='yes')))])">
      				<dc:subject>
      					<xsl:attribute name="rdf:resource">
      						<xsl:value-of select="lido:conceptID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]" />
      					</xsl:attribute>
      				</dc:subject>
      			</xsl:when>
      			<xsl:otherwise>      		
      				<!-- with only URI from Europeana dereferenceable vocab -->
      				<xsl:if test="(count(lido:conceptID[starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and not(lido:term)">
      					<dc:subject>
      						<xsl:attribute name="rdf:resource">
      							<xsl:value-of select="lido:conceptID[starts-with(., 'http://') or starts-with(., 'https://')]" />
      						</xsl:attribute>
      					</dc:subject>
      				</xsl:if>
      				<!-- no dereferenceable URI, provide term as Literal -->
      				<xsl:for-each select="lido:term[@lido:pref='preferred' or (not(@lido:pref) and not(@lido:addedSearchTerm='yes'))][string-length(text()) &gt; 0]">
      					<dc:subject>
      						<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
      							<xsl:attribute name="xml:lang">
      								<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
      							</xsl:attribute>
      						</xsl:if>			
      						<xsl:value-of select="." />
      					</dc:subject>				
      				</xsl:for-each>
      			</xsl:otherwise>
      		</xsl:choose>
      	</xsl:for-each>
      			
      	<!-- dc:subject : lido:subjectActor: Update 2020-02-02 -->
      	<xsl:for-each select="lido:descriptiveMetadata/lido:objectRelationWrap/lido:subjectWrap/lido:subjectSet/lido:subject/lido:subjectActor">

      		<xsl:choose>
      			<!-- dc:subject : lido:subjectActor with assigned preferred URI and additional information (multiple URIs or vital dates) for contextual class edm:Agent -->
      			<xsl:when test="(count(lido:actor/lido:actorID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and (lido:displayActor or lido:actor/lido:nameActorSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]) and ((lido:actor/lido:actorID[starts-with(@lido:pref, 'alternat')][starts-with(., 'http://') or starts-with(., 'https://')]) or (lido:actor/lido:vitalDatesActor[lido:earliestDate[string-length(text()) &gt; 0] or lido:latestDate[string-length(text()) &gt; 0]]))">
    						<dc:subject>
      							<xsl:attribute name="rdf:resource">
      								<xsl:value-of select="lido:actor/lido:actorID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]" />
      							</xsl:attribute>
      						</dc:subject>
      			</xsl:when>
      			<xsl:otherwise>      		
      				
      				<!-- dc:subject : lido:subjectActor with URI from a Europeana dereferenceable vocab, no additional information -->
      				<xsl:if test="(count(lido:actor/lido:actorID[starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and not(lido:displayActor or lido:actor/lido:nameActorSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)])">
      					<dc:subject>
      						<xsl:attribute name="rdf:resource">
      							<xsl:value-of select="lido:actor/lido:actorID[starts-with(., 'http://') or starts-with(., 'https://')]" />
      						</xsl:attribute>
      					</dc:subject>
      				</xsl:if>
      				
      				<!-- dc:subject Literal : lido:subjectActor with displayActor -->
      				<xsl:if test="lido:displayActor[string-length(text()) &gt; 0]">
      					<xsl:for-each select="lido:displayActor[string-length(text()) &gt; 0]">
      						<!-- include all labels for language variants -->
      							<dc:subject>
      								<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
      									<xsl:attribute name="xml:lang">
      										<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
      									</xsl:attribute>
      								</xsl:if>
      								<xsl:value-of select="." />
      							</dc:subject>
      					</xsl:for-each>
      				</xsl:if>
      				
      				<!-- dc:creator Literal : lido:eventActor without displayActor but nameActorSet -->
      				<xsl:if test="not(lido:displayActor[string-length(text()) &gt; 0]) and lido:actor/lido:nameActorSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]">
      					<xsl:for-each select="lido:actor/lido:nameActorSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)][string-length(text()) &gt; 0]">
      						<!-- include all labels for language variants -->
      							<dc:subject>
      								<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
      									<xsl:attribute name="xml:lang">
      										<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
      									</xsl:attribute>
      								</xsl:if>
      								<xsl:value-of select="." />
      							</dc:subject>
      					</xsl:for-each>
      				</xsl:if>	
      				
      			</xsl:otherwise>
      		</xsl:choose>
      		
      	</xsl:for-each>
      	
        <!-- dc:title : lido:titleSet / @lido:pref=preferred or empty -->
      	<xsl:for-each select="lido:descriptiveMetadata/lido:objectIdentificationWrap/lido:titleWrap/lido:titleSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)][string-length(text()) &gt; 0]">
          <dc:title>
          	<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
        				<xsl:attribute name="xml:lang">
        					<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
        				</xsl:attribute>
    				</xsl:if>
            <xsl:value-of select="." />
          </dc:title>
        </xsl:for-each>
        <!-- dc:title -->
        
      	<!-- dc:type : lido:objectWorkType and lido:classification : Update 2020-02-02 -->
      	<xsl:for-each select="lido:descriptiveMetadata/lido:objectClassificationWrap/lido:objectWorkTypeWrap/lido:objectWorkType | 
      		lido:descriptiveMetadata/lido:objectClassificationWrap/lido:classificationWrap/lido:classification[
      		not(@lido:type='language') and not (@lido:type='europeana:project') and not(lido:term[(. = 'IMAGE') or (. = 'VIDEO') or (. = 'TEXT') or (. = '3D') or (. = 'SOUND')])
      		]">
      			<xsl:choose>
      				<!-- with assigned preferred URI and additional information for contextual class skos:Concept -->
      				<xsl:when test="(count(lido:conceptID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and (lido:conceptID[starts-with(@lido:pref, 'alternat')][starts-with(., 'http://') or starts-with(., 'https://')]) and (lido:term[((@lido:pref='preferred') or (not(@lido:pref) and not(@lido:addedSearchTerm='yes')))])">
      						<dc:type>
      							<xsl:attribute name="rdf:resource">
      								<xsl:value-of select="lido:conceptID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]" />
      							</xsl:attribute>
      						</dc:type>
      				</xsl:when>
      				<xsl:otherwise>      		
      				<!-- with only URI from Europeana dereferenceable vocab -->
      				<xsl:if test="(count(lido:conceptID[starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and not(lido:term)">
      					<dc:type>
      						<xsl:attribute name="rdf:resource">
      							<xsl:value-of select="lido:conceptID[starts-with(., 'http://') or starts-with(., 'https://')]" />
      						</xsl:attribute>
      					</dc:type>
      				</xsl:if>
      				<!-- no dereferenceable URI, provide term as Literal -->
			       	<xsl:for-each select="lido:term[@lido:pref='preferred' or (not(@lido:pref) and not(@lido:addedSearchTerm='yes'))][string-length(text()) &gt; 0]">
            	    <dc:type>
                		<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
        					<xsl:attribute name="xml:lang">
        						<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
        					</xsl:attribute>
    					</xsl:if>			
						<xsl:value-of select="." />
					</dc:type>				
					</xsl:for-each>
      			</xsl:otherwise>
      		</xsl:choose>
      	</xsl:for-each>

   		<!-- dcterms:alternative : lido:titleSet / @lido:pref=alternative  -->
      	<xsl:for-each select="lido:descriptiveMetadata/lido:objectIdentificationWrap/lido:titleWrap/lido:titleSet/lido:appellationValue[starts-with(@lido:pref, 'alternat')][string-length(text()) &gt; 0]">
          <dcterms:alternative>
          	<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
        				<xsl:attribute name="xml:lang">
        					<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
        				</xsl:attribute>
    				</xsl:if>
            <xsl:value-of select="." />
          </dcterms:alternative>
        </xsl:for-each>
        <!-- dcterms:alternative -->
           		
   		<!-- dct:conformsTo -->
        <xsl:for-each select="lido:descriptiveMetadata/lido:objectRelationWrap/lido:relatedWorksWrap/lido:relatedWorkSet">
         <xsl:if test="lido:relatedWorkRelType/lido:conceptID='http://purl.org/dc/terms/conformsTo' and (lido:relatedWork/lido:object/lido:objectWebResource or lido:relatedWork/lido:object/lido:objectID or lido:relatedWork/lido:object/lido:objectNote)">
         	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectWebResource[string-length(text()) &gt; 0]">  
            <xsl:if test="starts-with(.,'http')">
                <dcterms:conformsTo>
                <xsl:attribute name="rdf:resource">
        			<xsl:value-of select="." />
                </xsl:attribute>    
        		</dcterms:conformsTo>
        	</xsl:if>
        	</xsl:for-each>
         	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectID[string-length(text()) &gt; 0]">
            	<dcterms:conformsTo>
        			<xsl:value-of select="." />
        		</dcterms:conformsTo>
        	</xsl:for-each>
         	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectNote[string-length(text()) &gt; 0]">
                <dcterms:conformsTo>
                	<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
                		<xsl:attribute name="xml:lang">
                			<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
                		</xsl:attribute>
                	</xsl:if>
                	<xsl:value-of select="." />
        		</dcterms:conformsTo>
        	</xsl:for-each>
        
          </xsl:if>
        </xsl:for-each>
   		
   		<!-- dcterms:created : lido:eventDate with lido:eventType = production or creation or designing -->
   		<!-- dcterms:created -->
        <xsl:for-each select="lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event">
        <xsl:if test="(lido:eventType/lido:conceptID = 'http://terminology.lido-schema.org/lido00007') or (lido:eventType/lido:conceptID = 'http://terminology.lido-schema.org/lido00012') or (lido:eventType/lido:conceptID = 'http://terminology.lido-schema.org/lido00224')">
        <xsl:for-each select="lido:eventDate">
        	<xsl:if test="lido:date/lido:earliestDate[string-length(text()) &gt; 0] | lido:displayDate[string-length(text()) &gt; 0]">
            <dcterms:created>
				<xsl:choose>
					<xsl:when test="lido:date/lido:earliestDate = lido:date/lido:latestDate">
						<xsl:attribute name="xml:lang">
							<xsl:value-of select="lido:langSelect($descLang,lido:date/lido:earliestDate[1]/@xml:lang)" />
						</xsl:attribute>
						<xsl:value-of select="lido:date/lido:earliestDate" />
					</xsl:when>
					<xsl:when test="lido:date/lido:earliestDate">
						<xsl:attribute name="xml:lang">
							<xsl:value-of select="lido:langSelect($descLang,lido:date/lido:earliestDate[1]/@xml:lang)" />
						</xsl:attribute>
						<xsl:value-of select="concat(lido:date/lido:earliestDate, '/', lido:date/lido:latestDate)" />
					</xsl:when>
					<xsl:otherwise>
						<xsl:if test="string-length( lido:langSelect($descLang,lido:displayDate[1]/@xml:lang)) &gt; 0">
							<xsl:attribute name="xml:lang">
								<xsl:value-of select="lido:langSelect($descLang,lido:displayDate[1]/@xml:lang)" />
							</xsl:attribute>
						</xsl:if>	
						<xsl:value-of select="lido:displayDate[string-length(text()) &gt; 0]" />
					</xsl:otherwise>
				</xsl:choose>
            </dcterms:created>
            </xsl:if>
        </xsl:for-each>
        </xsl:if>
        </xsl:for-each>
        <!-- dcterms:created -->
        
        <!-- dcterms:extent : lido:objectMeasurementsSet -->
		<!-- 2017-07-08 RStein: xml:lang attribute added for dcterms:extent -->
        <xsl:for-each select="lido:descriptiveMetadata/lido:objectIdentificationWrap/lido:objectMeasurementsWrap/lido:objectMeasurementsSet[lido:displayObjectMeasurements or lido:objectMeasurements/lido:measurementsSet]">
          <dcterms:extent>
			<xsl:choose>
				<xsl:when test="lido:displayObjectMeasurements[string-length(text()) &gt; 0]">
        			<xsl:if test="string-length( lido:langSelect($descLang,lido:displayObjectMeasurements[1]/@xml:lang)) &gt; 0">
        				<xsl:attribute name="xml:lang">
        					<xsl:value-of select="lido:langSelect($descLang,lido:displayObjectMeasurements[1]/@xml:lang)" />
        				</xsl:attribute>
    				</xsl:if>	
					<xsl:value-of select="lido:displayObjectMeasurements[string-length(text()) &gt; 0][1]/text()" />
				</xsl:when>
				<xsl:otherwise>
					<xsl:for-each select="lido:objectMeasurements/lido:measurementsSet[string-length(lido:measurementValue)&gt;0]">
						<xsl:value-of select="concat(lido:measurementType, ': ', lido:measurementValue, ' ', lido:measurementUnit)" />
						<xsl:for-each select="../lido:extentMeasurements[string-length(.)&gt;0]"><xsl:value-of select="concat(' (', ., ')')" /></xsl:for-each>
					</xsl:for-each>
				</xsl:otherwise>
			</xsl:choose>
          </dcterms:extent>
        </xsl:for-each>
        <!-- dcterms:extent -->
        
        <!-- No LIDO property is mapped to the following properties of EDM -->
        <!-- dcterms:hasFormat -->
		<xsl:for-each select="lido:descriptiveMetadata/lido:objectRelationWrap/lido:relatedWorksWrap/lido:relatedWorkSet">
            <xsl:if test="lido:relatedWorkRelType/lido:conceptID = 'http://purl.org/dc/terms/hasFormat' and                (lido:relatedWork/lido:object/lido:objectWebResource or lido:relatedWork/lido:object/lido:objectID or                                 lido:relatedWork/lido:object/lido:objectNote)">
            	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectWebResource[string-length(text()) &gt; 0]">  
            <xsl:if test="starts-with(.,'http')">
                <dcterms:hasFormat>
                <xsl:attribute name="rdf:resource">
        			<xsl:value-of select="." />
                </xsl:attribute>    
        		</dcterms:hasFormat>
        	</xsl:if>
        	</xsl:for-each>
            	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectID[string-length(text()) &gt; 0]">
            	<dcterms:hasFormat>
        			<xsl:value-of select="." />
        		</dcterms:hasFormat>
        	</xsl:for-each>
            	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectNote[string-length(text()) &gt; 0]">
                <dcterms:hasFormat>
                	<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
                		<xsl:attribute name="xml:lang">
                			<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
                		</xsl:attribute>
                	</xsl:if>
                	<xsl:value-of select="." />
        		</dcterms:hasFormat>
        	</xsl:for-each>
        	</xsl:if>
        </xsl:for-each>
        
        
        <!-- dcterms:hasPart -->
        <xsl:for-each select="lido:descriptiveMetadata/lido:objectRelationWrap/lido:relatedWorksWrap/lido:relatedWorkSet">
        	<xsl:if test="lido:relatedWorkRelType/lido:conceptID = 'http://purl.org/dc/terms/hasPart' and                (lido:relatedWork/lido:object/lido:objectWebResource or lido:relatedWork/lido:object/lido:objectID or                                 lido:relatedWork/lido:object/lido:objectNote)">
            <xsl:for-each select="lido:relatedWork/lido:object/lido:objectWebResource">
            <xsl:if test="lido:relatedWork/lido:object/lido:objectWebResource[starts-with(.,'http')]">
        		<dcterms:hasPart>
                <xsl:attribute name="rdf:resource">
        			<xsl:value-of select="." />
                </xsl:attribute>    
        		</dcterms:hasPart>
        	</xsl:if>
        	</xsl:for-each>
        		<xsl:for-each select="lido:relatedWork/lido:object/lido:objectID[string-length(text()) &gt; 0]">
            	<dcterms:hasPart>
        			<xsl:value-of select="." />
        		</dcterms:hasPart>
        	</xsl:for-each>
        		<xsl:for-each select="lido:relatedWork/lido:object/lido:objectNote[string-length(text()) &gt; 0]">
                <dcterms:hasPart>
                	<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
                		<xsl:attribute name="xml:lang">
                			<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
                		</xsl:attribute>
                	</xsl:if>
                	<xsl:value-of select="." />
        		</dcterms:hasPart>
        	</xsl:for-each>
        	</xsl:if>
        </xsl:for-each>
        <!-- dcterms:hasVersion -->
        <!-- dcterms:isFormatOf -->
        <!-- dcterms:isPartOf -->
        <xsl:for-each select="lido:descriptiveMetadata/lido:objectRelationWrap/lido:relatedWorksWrap/lido:relatedWorkSet">
        	<xsl:if test="lido:relatedWorkRelType/lido:conceptID = 'http://purl.org/dc/terms/isPartOf' and                (lido:relatedWork/lido:object/lido:objectWebResource or lido:relatedWork/lido:object/lido:objectID or                                 lido:relatedWork/lido:object/lido:objectNote)">
        		<xsl:for-each select="lido:relatedWork/lido:object/lido:objectWebResource[string-length(text()) &gt; 0]">
            <xsl:if test="starts-with(.,'http')">
        		<dcterms:isPartOf>
                <xsl:attribute name="rdf:resource">
        			<xsl:value-of select="." />
                </xsl:attribute>    
        		</dcterms:isPartOf>
        	</xsl:if>
        	</xsl:for-each>
        		<xsl:for-each select="lido:relatedWork/lido:object/lido:objectID[string-length(text()) &gt; 0]">
            	<dcterms:isPartOf>
        			<xsl:value-of select="." />
        		</dcterms:isPartOf>
        	</xsl:for-each>
        		<xsl:for-each select="lido:relatedWork/lido:object/lido:objectNote[string-length(text()) &gt; 0]">
                <dcterms:isPartOf>
                	<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
                		<xsl:attribute name="xml:lang">
                			<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
                		</xsl:attribute>
                	</xsl:if>
                	<xsl:value-of select="." />
        		</dcterms:isPartOf>
        	</xsl:for-each>
        	</xsl:if>
        </xsl:for-each>
        <!-- dcterms:isReferencedBy -->
        <!-- dcterms:isReplacedBy -->
		<!-- dcterms:isReplacedBy -->
		<!-- dcterms:RequiredBy -->
		<!-- dcterms:issued -->
		<!-- dcterms:isVersionOf -->
        
      	<!-- dcterms:medium : lido:eventMaterialsTech//lido:termMaterials / @lido:type=material: Update 2020-02-02 -->
      	<xsl:for-each select="lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event/lido:eventMaterialsTech/lido:materialsTech/lido:termMaterialsTech[lower-case(@lido:type)='material']">
      		<xsl:choose>
      			<!-- with assigned preferred URI and additional information for contextual class skos:Concept -->
      			<xsl:when test="(count(lido:conceptID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and (lido:conceptID[starts-with(@lido:pref, 'alternat')][starts-with(., 'http://') or starts-with(., 'https://')]) and (lido:term[((@lido:pref='preferred') or (not(@lido:pref) and not(@lido:addedSearchTerm='yes')))])">
      				<dcterms:medium>
      					<xsl:attribute name="rdf:resource">
      						<xsl:value-of select="lido:conceptID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]" />
      					</xsl:attribute>
      				</dcterms:medium>
      			</xsl:when>
      			<xsl:otherwise>      		
      				<!-- with only URI from Europeana dereferenceable vocab -->
      				<xsl:if test="(count(lido:conceptID[starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and not(lido:term)">
      					<dcterms:medium>
      						<xsl:attribute name="rdf:resource">
      							<xsl:value-of select="lido:conceptID[starts-with(., 'http://') or starts-with(., 'https://')]" />
      						</xsl:attribute>
      					</dcterms:medium>
      				</xsl:if>
      				<!-- no dereferenceable URI, provide term as Literal -->
      				<xsl:for-each select="lido:term[@lido:pref='preferred' or (not(@lido:pref) and not(@lido:addedSearchTerm='yes'))][string-length(text()) &gt; 0]">
      					<dcterms:medium>
      						<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
      							<xsl:attribute name="xml:lang">
      								<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
      							</xsl:attribute>
      						</xsl:if>			
      						<xsl:value-of select="." />
      					</dcterms:medium>				
      				</xsl:for-each>
      			</xsl:otherwise>
      		</xsl:choose>
      	</xsl:for-each>

        <!-- dcterms:provenance : lido:repositorySet[@lido:type='current'] -> lido:repositoryName and/or lido:repositoryLocation -->
        <!-- First approach was:
				IF NOT lido:repositoryName THEN dcterms:spatial : lido:repositorySet/lido:repositoryLocation
				changed by request of Europeana
		-->
		<!-- 2017-07-08 RStein: xml:lang attribute added for dcterms:provenance -->
        <xsl:for-each select="lido:descriptiveMetadata/lido:objectIdentificationWrap/lido:repositoryWrap/lido:repositorySet[@lido:type='current']">
        	<xsl:if test="lido:repositoryName/lido:legalBodyName/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)][string-length(text()) &gt; 0] or lido:repositoryLocation/lido:namePlaceSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)][string-length(text()) &gt; 0]">
				<xsl:choose>
					<xsl:when test="lido:repositoryName/lido:legalBodyName/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)][string-length(text()) &gt; 0] and lido:repositoryLocation/lido:namePlaceSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)][string-length(text()) &gt; 0]">
					<dcterms:provenance>
        			<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
        				<xsl:attribute name="xml:lang">
        					<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
        				</xsl:attribute>
    				</xsl:if>	
						<xsl:value-of select="concat(lido:repositoryName[1]/lido:legalBodyName[1]/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)][string-length(text()) &gt; 0][1], ', ', lido:repositoryLocation[1]/lido:namePlaceSet[1]/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)][string-length(text()) &gt; 0][1])" />
					</dcterms:provenance>
					</xsl:when>
					<xsl:when test="lido:repositoryName/lido:legalBodyName/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)][string-length(text()) &gt; 0]">
					<dcterms:provenance>
        			<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
        				<xsl:attribute name="xml:lang">
        					<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
        				</xsl:attribute>
    				</xsl:if>	
						<xsl:value-of select="lido:repositoryName/lido:legalBodyName/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)][string-length(text()) &gt; 0][1]" />    
					</dcterms:provenance>
					</xsl:when>
					<xsl:when test="lido:repositoryLocation/lido:namePlaceSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)][string-length(text()) &gt; 0]">
					<dcterms:provenance>
        			<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
        				<xsl:attribute name="xml:lang">
        					<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
        				</xsl:attribute>
    				</xsl:if>	
						<xsl:value-of select="lido:repositoryLocation/lido:namePlaceSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)][string-length(text()) &gt; 0][1]" />
					</dcterms:provenance>
					</xsl:when>
				</xsl:choose>
			</xsl:if>
        </xsl:for-each>
        <!-- dcterms:provenance -->
        
         <!-- dcterms:provenance : lido:repositorySet@lido:type='former' -> lido:repositoryName -->
        <xsl:for-each select="lido:descriptiveMetadata/lido:objectIdentificationWrap/lido:repositoryWrap/lido:repositorySet[@lido:type='former']">
			<xsl:if test="lido:repositoryName/lido:legalBodyName/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]">
				<xsl:for-each select="lido:repositoryName/lido:legalBodyName/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)][string-length(text()) &gt; 0]">
					<dcterms:provenance>
						<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
							<xsl:attribute name="xml:lang">
								<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
							</xsl:attribute>
						</xsl:if>
						<xsl:value-of select="normalize-space(text())" />
					</dcterms:provenance>
				</xsl:for-each>
			</xsl:if>
        </xsl:for-each>
        <!-- dcterms:provenance -->

       <!-- dct:references -->
        <xsl:for-each select="lido:descriptiveMetadata/lido:objectRelationWrap/lido:relatedWorksWrap/lido:relatedWorkSet">
            <xsl:if test="lido:relatedWorkRelType/lido:conceptID = 'http://purl.org/dc/terms/references' and                (lido:relatedWork/lido:object/lido:objectWebResource or lido:relatedWork/lido:object/lido:objectID or                                 lido:relatedWork/lido:object/lido:objectNote)">
            	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectWebResource[string-length(text()) &gt; 0]">  
            <xsl:if test="starts-with(.,'http')">
                <dcterms:references>
                <xsl:attribute name="rdf:resource">
        			<xsl:value-of select="." />
                </xsl:attribute>    
        		</dcterms:references>
        	</xsl:if>
        	</xsl:for-each>
            	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectID[string-length(text()) &gt; 0]">
            	<dcterms:references>
        			<xsl:value-of select="." />
        		</dcterms:references>
        	</xsl:for-each>
            	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectNote[string-length(text()) &gt; 0]">
                <dcterms:references>
        			<xsl:value-of select="." />
        		</dcterms:references>
        	</xsl:for-each>
        	</xsl:if>
        </xsl:for-each>
        
      	<!-- dcterms:spatial : lido:eventPlace from any lido:eventSet and lido:subjectPlace -->

      	<xsl:for-each select="lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event/lido:eventPlace | lido:descriptiveMetadata/lido:objectRelationWrap/lido:subjectWrap/lido:subjectSet/lido:subject/lido:subjectPlace">
      		
      		<xsl:choose>
      			<!-- dcterms:spatial : lido:subjectPlace with assigned preferred URI and additional information for contextual class edm:Place -->
      			<xsl:when test="(count(lido:place/lido:placeID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and (lido:displayPlace or lido:place/lido:namePlaceSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]) and (lido:place/lido:placeID[starts-with(@lido:pref, 'alternat')][starts-with(., 'http://') or starts-with(., 'https://')] or lido:place/lido:gml/gml:Point/gml:pos)">
      				<dcterms:spatial>
      					<xsl:attribute name="rdf:resource">
      						<xsl:value-of select="lido:place/lido:placeID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]" />
      					</xsl:attribute>
      				</dcterms:spatial>
      			</xsl:when>
      			<xsl:otherwise>      		
      				
      				<!-- dcterms:spatial : lido:subjectPlace with URI from a Europeana dereferenceable vocab, no additional information -->
      				<xsl:if test="(count(lido:place/lido:placeID[starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and not(lido:displayPlace or lido:place/lido:namePlaceSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)])">
      					<dcterms:spatial>
      						<xsl:attribute name="rdf:resource">
      							<xsl:value-of select="lido:place/lido:placeID[starts-with(., 'http://') or starts-with(., 'https://')]" />
      						</xsl:attribute>
      					</dcterms:spatial>
      				</xsl:if>
      				
      				<!-- dcterms:spatial Literal : lido:subjectPlace with displayActorInRole -->
      				<xsl:if test="lido:displayPlace[string-length(text()) &gt; 0]">
      					<xsl:for-each select="lido:displayPlace[string-length(text()) &gt; 0]">
      						<!-- include all labels for language variants -->
      						<dcterms:spatial>
      							<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
      								<xsl:attribute name="xml:lang">
      									<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
      								</xsl:attribute>
      							</xsl:if>
      							<xsl:value-of select="." />
      						</dcterms:spatial>
      					</xsl:for-each>
      				</xsl:if>
      				
      				<!-- dcterms:spatial Literal : lido:subjectPlace without displayPlace but namePlaceSet -->
      				<xsl:if test="not(lido:displayPlace[string-length(text()) &gt; 0]) and lido:place/lido:namePlaceSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]">
      					<xsl:for-each select="lido:place/lido:namePlaceSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)][string-length(text()) &gt; 0]">
      						<!-- include all labels for language variants -->
      						<dcterms:spatial>
      							<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
      								<xsl:attribute name="xml:lang">
      									<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
      								</xsl:attribute>
      							</xsl:if>
      							<xsl:value-of select="." />
      						</dcterms:spatial>
      					</xsl:for-each>
      				</xsl:if>	
      				
      			</xsl:otherwise>
      		</xsl:choose>
      		
      	</xsl:for-each>
      	
        <!-- No LIDO property is mapped to the following properties of EDM -->
        <!-- dct:tableOfContents -->
      	<xsl:for-each select="lido:descriptiveMetadata/lido:objectIdentificationWrap/lido:objectDescriptionWrap/lido:objectDescriptionSet[@lido:type='tableOfContents']/lido:descriptiveNoteValue[string-length(text()) &gt; 0]">
          <dcterms:tableOfContents>
            <xsl:value-of select="." />
          </dcterms:tableOfContents>
        </xsl:for-each>
        
        <!-- dct:temporal -->
        <xsl:for-each select="lido:descriptiveMetadata/lido:objectRelationWrap/lido:subjectWrap/lido:subjectSet/lido:subject/lido:subjectDate">
        	<xsl:if test="lido:date/lido:earliestDate[string-length(text()) &gt; 0] | lido:displayDate[string-length(text()) &gt; 0]">
        	<dcterms:temporal>
				<xsl:choose>
					<xsl:when test="lido:date/lido:earliestDate = lido:date/lido:latestDate">
						<xsl:attribute name="xml:lang">
							<xsl:value-of select="lido:langSelect($descLang,lido:date/lido:earliestDate[1]/@xml:lang)" />
						</xsl:attribute>
						<xsl:value-of select="lido:date/lido:earliestDate" />
					</xsl:when>
					<xsl:when test="lido:date/lido:earliestDate">
						<xsl:attribute name="xml:lang">
							<xsl:value-of select="lido:langSelect($descLang,lido:date/lido:earliestDate[1]/@xml:lang)" />
						</xsl:attribute>
						<xsl:value-of select="concat(lido:date/lido:earliestDate, '/', lido:date/lido:latestDate)" />
					</xsl:when>
					<xsl:otherwise>
						<xsl:if test="string-length( lido:langSelect($descLang,lido:displayDate[1]/@xml:lang)) &gt; 0">
							<xsl:attribute name="xml:lang">
								<xsl:value-of select="lido:langSelect($descLang,lido:displayDate[1]/@xml:lang)" />
							</xsl:attribute>
						</xsl:if>	
						<xsl:value-of select="lido:displayDate" />
					</xsl:otherwise>
				</xsl:choose>
			</dcterms:temporal>
		</xsl:if>
        </xsl:for-each>
        
      	<xsl:for-each select="lido:descriptiveMetadata/lido:objectClassificationWrap/lido:classificationWrap/lido:classification[@lido:type='temporal']/lido:term[string-length(text()) &gt; 0]">
          <dcterms:temporal>
			<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
				<xsl:attribute name="xml:lang">
					<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
				</xsl:attribute>
			</xsl:if>
            <xsl:value-of select="." />
          </dcterms:temporal>
        </xsl:for-each>
		
		<!-- edm:currentLocation -->
		<xsl:for-each select="lido:descriptiveMetadata/lido:objectIdentificationWrap/lido:repositoryWrap/lido:repositorySet[@lido:type='current']/lido:repositoryLocation/lido:placeID[@lido:type='URI']">
          <edm:currentLocation>
          	<xsl:attribute name="rdf:resource">
            	<xsl:value-of select="." />
            </xsl:attribute>  
          </edm:currentLocation>
        </xsl:for-each>
        
		
		<!-- edm:incorporates -->
		<xsl:for-each select="lido:descriptiveMetadata/lido:objectRelationWrap/lido:relatedWorksWrap/lido:relatedWorkSet">
            <xsl:if test="lido:relatedWorkRelType/lido:conceptID = 'http://www.europeana.eu/schemas/edm/incorporates' and                (lido:relatedWork/lido:object/lido:objectWebResource or lido:relatedWork/lido:object/lido:objectID or                                 lido:relatedWork/lido:object/lido:objectNote)">
            <xsl:for-each select="lido:relatedWork/lido:object/lido:objectWebResource">  
            <xsl:if test="starts-with(.,'http')">
                <edm:incorporates>
                <xsl:attribute name="rdf:resource">
        			<xsl:value-of select="." />
                </xsl:attribute>    
        		</edm:incorporates>
        	</xsl:if>
        	</xsl:for-each>
        	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectID">
            	<edm:incorporates>
        			<xsl:value-of select="." />
        		</edm:incorporates>
        	</xsl:for-each>
        	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectNote">
                <edm:incorporates>
        			<xsl:value-of select="." />
        		</edm:incorporates>
        	</xsl:for-each>
        	</xsl:if>
        </xsl:for-each>
        
		<!-- edm:isDerivativeOf -->
		<xsl:for-each select="lido:descriptiveMetadata/lido:objectRelationWrap/lido:relatedWorksWrap/lido:relatedWorkSet">
            <xsl:if test="lido:relatedWorkRelType/lido:conceptID = 'http://www.europeana.eu/schemas/edm/isDerivativeOf' and                (lido:relatedWork/lido:object/lido:objectWebResource or lido:relatedWork/lido:object/lido:objectID or                                 lido:relatedWork/lido:object/lido:objectNote)">
            	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectWebResource[string-length(text()) &gt; 0]">  
            <xsl:if test="starts-with(.,'http')">
                <edm:isDerivativeOf>
                <xsl:attribute name="rdf:resource">
        			<xsl:value-of select="." />
                </xsl:attribute>    
        		</edm:isDerivativeOf>
        	</xsl:if>
        	</xsl:for-each>
            	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectID[string-length(text()) &gt; 0]">
            	<edm:isDerivativeOf>
        			<xsl:value-of select="." />
        		</edm:isDerivativeOf>
        	</xsl:for-each>
            	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectNote[string-length(text()) &gt; 0]">
                <edm:isDerivativeOf>
        			<xsl:value-of select="." />
        		</edm:isDerivativeOf>
        	</xsl:for-each>
        	</xsl:if>
        </xsl:for-each>
		
		<!-- edm:isNextInSequence -->
		<xsl:for-each select="lido:descriptiveMetadata/lido:objectRelationWrap/lido:relatedWorksWrap/lido:relatedWorkSet">
            <xsl:if test="lido:relatedWorkRelType/lido:conceptID = 'http://www.europeana.eu/schemas/edm/isNextInSequence' and                (lido:relatedWork/lido:object/lido:objectWebResource or lido:relatedWork/lido:object/lido:objectID or                                 lido:relatedWork/lido:object/lido:objectNote)">
            	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectWebResource[string-length(text()) &gt; 0]">  
            <xsl:if test="starts-with(.,'http')">
                <edm:isNextInSequence>
                <xsl:attribute name="rdf:resource">
        			<xsl:value-of select="." />
                </xsl:attribute>    
        		</edm:isNextInSequence>
        	</xsl:if>
        	</xsl:for-each>
            	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectID[string-length(text()) &gt; 0]">
            	<edm:isNextInSequence>
        			<xsl:value-of select="." />
        		</edm:isNextInSequence>
        	</xsl:for-each>
            	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectNote[string-length(text()) &gt; 0]">
                <edm:isNextInSequence>
        			<xsl:value-of select="." />
        		</edm:isNextInSequence>
        	</xsl:for-each>
        	</xsl:if>
        </xsl:for-each>
        
		<!-- edm:isRelatedTo -->
		<xsl:for-each select="lido:descriptiveMetadata/lido:objectRelationWrap/lido:relatedWorksWrap/lido:relatedWorkSet">
            <xsl:if test="lido:relatedWorkRelType/lido:conceptID = 'http://www.europeana.eu/schemas/edm/isRelatedTo' and                (lido:relatedWork/lido:object/lido:objectWebResource or lido:relatedWork/lido:object/lido:objectID or                                 lido:relatedWork/lido:object/lido:objectNote)">
            	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectWebResource[string-length(text()) &gt; 0]">  
            <xsl:if test="starts-with(.,'http')">
                <edm:isRelatedTo>
                <xsl:attribute name="rdf:resource">
        			<xsl:value-of select="." />
                </xsl:attribute>    
        		</edm:isRelatedTo>
        	</xsl:if>
        	</xsl:for-each>
            	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectID[string-length(text()) &gt; 0]">
            	<edm:isRelatedTo>
        			<xsl:value-of select="." />
        		</edm:isRelatedTo>
        	</xsl:for-each>
            	<xsl:for-each select="lido:relatedWork/lido:object/lido:objectNote[string-length(text()) &gt; 0]">
                <edm:isRelatedTo>
        			<xsl:value-of select="." />
        		</edm:isRelatedTo>
        	</xsl:for-each>
        	</xsl:if>
        </xsl:for-each>
		
		<!-- edm:isRepresentationOf -->
		<!-- edm:isSimilarTo -->
		<!-- edm:isSuccessorOf -->
		<!-- edm:realizes -->
   
        <!-- edm:type : lido:classification / @lido:type=europeana:type -->        
        <xsl:if test="(lido:descriptiveMetadata/lido:objectClassificationWrap/lido:classificationWrap/lido:classification/lido:term = 'IMAGE') or (lido:descriptiveMetadata/lido:objectClassificationWrap/lido:classificationWrap/lido:classification/lido:term = 'VIDEO') or (lido:descriptiveMetadata/lido:objectClassificationWrap/lido:classificationWrap/lido:classification/lido:term = 'TEXT') or (lido:descriptiveMetadata/lido:objectClassificationWrap/lido:classificationWrap/lido:classification/lido:term = '3D') or (lido:descriptiveMetadata/lido:objectClassificationWrap/lido:classificationWrap/lido:classification/lido:term = 'SOUND')">
          <xsl:for-each select="lido:descriptiveMetadata/lido:objectClassificationWrap/lido:classificationWrap/lido:classification/lido:term[(. = 'IMAGE') or (. = 'VIDEO') or (. = 'TEXT') or (. = '3D') or (. = 'SOUND')]">
            <xsl:if test="position() = 1">
              <xsl:if test="index-of($var0/item, normalize-space()) &gt; 0">
                <edm:type>
                  <xsl:value-of select="." />
                </edm:type>
              </xsl:if>
            </xsl:if>
          </xsl:for-each>
        </xsl:if>
        	
        <!-- owl:sameAs -->
        <!-- No LIDO property is mapped to owl:sameAs of EDM -->
      </edm:ProvidedCHO>
      
	  <!-- edm:WebResource : lido:recordInfoLink -->
      <xsl:for-each select="lido:administrativeMetadata/lido:recordWrap/lido:recordInfoSet/lido:recordInfoLink[starts-with(., 'http://') or starts-with(., 'https://')]">
        <edm:WebResource>
            <xsl:attribute name="rdf:about">
                <xsl:if test="position() = 1">
                  <xsl:value-of select="." />
                </xsl:if>
            </xsl:attribute>
        </edm:WebResource>
      </xsl:for-each>
      <!-- edm:WebResource -->
      
      <!-- edm:WebResource : lido:resourceSet -->
      <xsl:for-each select="lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet[lido:resourceRepresentation/lido:linkResource]">
        <xsl:if test="lido:resourceRepresentation/lido:linkResource[starts-with(., 'http://') or starts-with(., 'https://')]">
        <xsl:for-each select="lido:resourceRepresentation[not(@lido:type='iiif_image_api')]">
        <edm:WebResource>    
            <xsl:attribute name="rdf:about">
            <xsl:for-each select="lido:linkResource[starts-with(., 'http://') or starts-with(., 'https://')]">
                <xsl:if test="position() = 1">
                  <xsl:value-of select="." />
                </xsl:if>
            </xsl:for-each>
            </xsl:attribute>
				<xsl:for-each select="lido:rightsResource"> 
				  <xsl:choose>
						<xsl:when test="lido:creditLine">
							<dc:rights>
								<xsl:value-of select="lido:creditLine" />
							</dc:rights>
						</xsl:when>
						<xsl:when test="lido:rightsHolder/lido:legalBodyName/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]">
						<xsl:for-each select="lido:rightsHolder/lido:legalBodyName/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]">
							<dc:rights>
								<xsl:value-of select="." />
							 </dc:rights>
						</xsl:for-each>
						</xsl:when>
					</xsl:choose>
				</xsl:for-each>
                
                <xsl:for-each select="lido:resourceMeasurementsSet"> 
                
                <xsl:if test="lido:measurementType = 'http://www.europeana.eu/schemas/edm/isNextInSequence' and                   lido:measurementUnit ='URI'">            
					<edm:isNextInSequence>
                        <xsl:attribute name="rdf:resource">
							  <xsl:value-of select="lido:measurementValue" />
                        </xsl:attribute>
					</edm:isNextInSequence>
                </xsl:if>
				</xsl:for-each>      
        </edm:WebResource>
        </xsl:for-each>

        <xsl:for-each select="lido:resourceRepresentation[@lido:type='iiif_image_api']">
		<xsl:variable name="linkService" select="lido:linkResource[starts-with(., 'http://') or starts-with(., 'https://')][1]/text()" />
        <edm:WebResource>    
            <xsl:attribute name="rdf:about">
                  <xsl:value-of select="concat($linkService, '/full/full/0/default.jpg')" />
            </xsl:attribute>
            <svcs:has_service>
				<xsl:attribute name="rdf:resource" select="$linkService" />
            </svcs:has_service>
            
				<xsl:for-each select="lido:rightsResource"> 
				  <xsl:choose>
						<xsl:when test="lido:creditLine">
							<dc:rights>
								<xsl:value-of select="lido:creditLine" />
							</dc:rights>
						</xsl:when>
						<xsl:when test="lido:rightsHolder/lido:legalBodyName/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]">
						<xsl:for-each select="lido:rightsHolder/lido:legalBodyName/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]">
							<dc:rights>
								<xsl:value-of select="." />
							 </dc:rights>
						</xsl:for-each>
						</xsl:when>
					</xsl:choose>
				</xsl:for-each>
        </edm:WebResource>
        <svcs:Service>
			<xsl:attribute name="rdf:about" select="$linkService" />
			<dcterms:conformsTo rdf:resource="http://iiif.io/api/image" />
        </svcs:Service>
        </xsl:for-each>
        </xsl:if>
      </xsl:for-each>
      <!-- edm:WebResource -->

      <!-- edm:Agent : lido:eventActor --> 
      <xsl:for-each select="lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event/lido:eventActor">
      	<xsl:if test="(count(lido:actorInRole/lido:actor/lido:actorID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and (lido:displayActorInRole or lido:actorInRole/lido:actor/lido:nameActorSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]) and ((lido:actorInRole/lido:actor/lido:actorID[starts-with(@lido:pref, 'alternat')][starts-with(., 'http://') or starts-with(., 'https://')]) or (lido:actorInRole/lido:actor/lido:vitalDatesActor[lido:earliestDate[string-length(text()) &gt; 0] or lido:latestDate[string-length(text()) &gt; 0]]))">
      		<edm:Agent>
            <xsl:attribute name="rdf:about">
            	<xsl:value-of select="lido:actorInRole/lido:actor/lido:actorID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]" />
            </xsl:attribute>
      			<!-- include prefLabel from input record : displayActorInRole -->
     			<xsl:if test="lido:displayActorInRole">
      				<xsl:for-each select="lido:displayActorInRole">
      					<!-- include all labels for language variants -->
      						<skos:prefLabel>
      							<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
      								<xsl:attribute name="xml:lang">
      									<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
      								</xsl:attribute>
      							</xsl:if>
      							<xsl:value-of select="." />
      						</skos:prefLabel>
      				</xsl:for-each>
      			</xsl:if>
      			<!-- include prefLabel from input record : no displayActorInRole, but nameActorSet -->
      			<xsl:if test="not(lido:displayActorInRole) and lido:actorInRole/lido:actor/lido:nameActorSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]">
      				<xsl:for-each select="lido:actorInRole/lido:actor/lido:nameActorSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]">
      					<!-- include all labels for language variants -->
    						<skos:prefLabel>
      							<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
      								<xsl:attribute name="xml:lang">
      									<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
      								</xsl:attribute>
      							</xsl:if>
      							<xsl:value-of select="." />
      						</skos:prefLabel>
      				</xsl:for-each>
      			</xsl:if>	
        	
        	<!-- edm:begin / edm:end -->
      			<xsl:for-each select="lido:actorInRole/lido:actor/lido:vitalDatesActor/lido:earliestDate[string-length(text()) &gt; 0]">
        		<edm:begin>
       				<xsl:value-of select=".[1]/text()" />
        		</edm:begin>
        	</xsl:for-each>
      			<xsl:for-each select="lido:actorInRole/lido:actor/lido:vitalDatesActor/lido:latestDate[string-length(text()) &gt; 0]">
        		<edm:end>
       				<xsl:value-of select=".[1]/text()" />
        		</edm:end>
        	</xsl:for-each>
        	
        	<!-- owl:sameAs -->
      			<xsl:for-each select="lido:actorInRole/lido:actor/lido:actorID[starts-with(@lido:pref, 'alternat')][starts-with(., 'http://') or starts-with(., 'https://')]">
        		<owl:sameAs>
        			<xsl:attribute name="rdf:resource">
        				<xsl:value-of select="." />
        			</xsl:attribute>
        		</owl:sameAs>
        	</xsl:for-each>
        </edm:Agent>
        </xsl:if>
      </xsl:for-each>
      <!-- edm:Agent --> 
      
  	<!-- edm:Agent : lido:subjectActor --> 
  	<xsl:for-each select="lido:descriptiveMetadata/lido:objectRelationWrap/lido:subjectWrap/lido:subjectSet/lido:subject/lido:subjectActor">
  		<xsl:if test="(count(lido:actor/lido:actorID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and (lido:displayActor or lido:actor/lido:nameActorSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]) and ((lido:actor/lido:actorID[starts-with(@lido:pref, 'alternat')][starts-with(., 'http://') or starts-with(., 'https://')]) or (lido:actor/lido:vitalDatesActor[lido:earliestDate[string-length(text()) &gt; 0] or lido:latestDate[string-length(text()) &gt; 0]]))">
  			<edm:Agent>
  				<xsl:attribute name="rdf:about">
  					<xsl:value-of select="lido:actor/lido:actorID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]" />
  				</xsl:attribute>
  				<!-- include prefLabel from input record : displayActor -->
  				<xsl:if test="lido:displayActor">
  					<xsl:for-each select="lido:displayActor">
  						<!-- include all labels for language variants -->
  						<skos:prefLabel>
  							<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
  								<xsl:attribute name="xml:lang">
  									<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
  								</xsl:attribute>
  							</xsl:if>
  							<xsl:value-of select="." />
  						</skos:prefLabel>
  					</xsl:for-each>
  				</xsl:if>
  				<!-- include prefLabel from input record : no displayActor, but nameActorSet -->
  				<xsl:if test="not(lido:displayActor) and lido:actor/lido:nameActorSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]">
  					<xsl:for-each select="lido:actor/lido:nameActorSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]">
  						<!-- include all labels for language variants -->
  						<skos:prefLabel>
  							<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
  								<xsl:attribute name="xml:lang">
  									<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
  								</xsl:attribute>
  							</xsl:if>
  							<xsl:value-of select="." />
  						</skos:prefLabel>
  					</xsl:for-each>
  				</xsl:if>	
  				
  				<!-- edm:begin / edm:end -->
  				<xsl:for-each select="lido:actor/lido:vitalDatesActor/lido:earliestDate[string-length(text()) &gt; 0]">
  					<edm:begin>
  						<xsl:value-of select=".[1]/text()" />
  					</edm:begin>
  				</xsl:for-each>
  				<xsl:for-each select="lido:actor/lido:vitalDatesActor/lido:latestDate[string-length(text()) &gt; 0]">
  					<edm:end>
  						<xsl:value-of select=".[1]/text()" />
  					</edm:end>
  				</xsl:for-each>
  				
  				<!-- owl:sameAs -->
  				<xsl:for-each select="lido:actor/lido:actorID[starts-with(@lido:pref, 'alternat')][starts-with(., 'http://') or starts-with(., 'https://')]">
  					<owl:sameAs>
  						<xsl:attribute name="rdf:resource">
  							<xsl:value-of select="." />
  						</xsl:attribute>
  					</owl:sameAs>
  				</xsl:for-each>
  			</edm:Agent>
  		</xsl:if>
  	</xsl:for-each>
  	<!-- edm:Agent --> 
  	
  	<!-- edm:Place : lido:eventPlace or lido:subjectPlace --> 
      <xsl:for-each select="lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event/lido:eventPlace | lido:descriptiveMetadata/lido:objectRelationWrap/lido:subjectWrap/lido:subjectSet/lido:subject/lido:subjectPlace">
      	<xsl:if test="(count(lido:place/lido:placeID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and (lido:displayPlace or lido:place/lido:namePlaceSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]) and (lido:place/lido:placeID[starts-with(@lido:pref, 'alternat')][starts-with(., 'http://') or starts-with(., 'https://')] or lido:place/lido:gml/gml:Point/gml:pos)"> 
       <edm:Place>
            <xsl:attribute name="rdf:about">
                	<xsl:value-of select="lido:place/lido:placeID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]" />
            </xsl:attribute>
       	<xsl:for-each select="lido:place/lido:gml/gml:Point/gml:pos">
       		<xsl:if test="@srsName and contains(.[1]/text(), ' ')">
       			<wgs84_pos:lat><xsl:value-of select="substring-after(.[1]/text(), ' ')"/></wgs84_pos:lat>
       			<wgs84_pos:long><xsl:value-of select="substring-before(.[1]/text(), ' ')"/></wgs84_pos:long>
       		</xsl:if>
       	</xsl:for-each>
       	<!-- include prefLabel from input record -->
       	<xsl:if test="lido:displayPlace">
       		<xsl:for-each select="lido:displayPlace">
       			<!-- include all labels for language variants -->
       			<skos:prefLabel>
       				<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
       					<xsl:attribute name="xml:lang">
       						<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
       					</xsl:attribute>
       				</xsl:if>
       				<xsl:value-of select="." />
       			</skos:prefLabel>
       		</xsl:for-each>
       	</xsl:if>
       	<xsl:if test="not(lido:displayPlace) and lido:place/lido:namePlaceSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]">
       		<xsl:for-each select="lido:place/lido:namePlaceSet/lido:appellationValue[@lido:pref='preferred' or not(@lido:pref)]">
       			<!-- include all labels for language variants -->
       			<skos:prefLabel>
       				<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
       					<xsl:attribute name="xml:lang">
       						<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
       					</xsl:attribute>
       				</xsl:if>
       				<xsl:value-of select="." />
       			</skos:prefLabel>
       		</xsl:for-each>
       	</xsl:if>	
       	
       	<xsl:for-each select="lido:place/lido:placeID[starts-with(@lido:pref, 'alternat')][starts-with(., 'http://') or starts-with(., 'https://')]">
       		<owl:sameAs>
       			<xsl:attribute name="rdf:resource">
       				<xsl:value-of select="." />
       			</xsl:attribute>
       		</owl:sameAs>
       	</xsl:for-each>
       </edm:Place>
        </xsl:if>
      </xsl:for-each> 
      <!-- edm:Place --> 
      
      <!-- edm:TimeSpan -->
      <!-- No LIDO property is mapped to this EDM class -->
      
  	<!-- skos:Concept : [lido:conceptID[@lido:pref='preferred']] for lido:objectWorkType or lido:classification or lido:termMaterialsTech or lido:subjectConcept --> 
  	<xsl:for-each select="
  		lido:descriptiveMetadata/lido:objectClassificationWrap/lido:objectWorkTypeWrap/lido:objectWorkType[(count(lido:conceptID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and (lido:conceptID[starts-with(@lido:pref, 'alternat')][starts-with(., 'http://') or starts-with(., 'https://')]) and (lido:term[((@lido:pref='preferred') or (not(@lido:pref) and not(@lido:addedSearchTerm='yes')))])] |
  		lido:descriptiveMetadata/lido:objectClassificationWrap/lido:classificationWrap/lido:classification[not(starts-with(@lido:type, 'europeana'))][(count(lido:conceptID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and (lido:conceptID[starts-with(@lido:pref, 'alternat')][starts-with(., 'http://') or starts-with(., 'https://')]) and (lido:term[((@lido:pref='preferred') or (not(@lido:pref) and not(@lido:addedSearchTerm='yes')))])] |
  		lido:descriptiveMetadata/lido:eventWrap/lido:eventSet/lido:event/lido:eventMaterialsTech/lido:materialsTech/lido:termMaterialsTech[(count(lido:conceptID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and (lido:conceptID[starts-with(@lido:pref, 'alternat')][starts-with(., 'http://') or starts-with(., 'https://')]) and (lido:term[((@lido:pref='preferred') or (not(@lido:pref) and not(@lido:addedSearchTerm='yes')))])] |
  		lido:descriptiveMetadata/lido:objectRelationWrap/lido:subjectWrap/lido:subjectSet/lido:subject/lido:subjectConcept[(count(lido:conceptID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]) eq 1) and (lido:conceptID[starts-with(@lido:pref, 'alternat')][starts-with(., 'http://') or starts-with(., 'https://')]) and (lido:term[((@lido:pref='preferred') or (not(@lido:pref) and not(@lido:addedSearchTerm='yes')))])]
  		">
  			<skos:Concept>
  				<xsl:attribute name="rdf:about">
  					<xsl:value-of select="lido:conceptID[@lido:pref='preferred'][starts-with(., 'http://') or starts-with(., 'https://')]" />
  				</xsl:attribute>
  				<xsl:for-each select="lido:term[((@lido:pref='preferred') or (not(@lido:pref) and not(@lido:addedSearchTerm='yes')))]">
  					<skos:prefLabel>
  						<xsl:if test="string-length(lido:langSelect($descLang,@xml:lang)) &gt; 0">
  							<xsl:attribute name="xml:lang">
  								<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
  							</xsl:attribute>
  						</xsl:if>
  						<xsl:value-of select="." />
  					</skos:prefLabel>
  				</xsl:for-each>
  				<xsl:for-each select="lido:term[((starts-with(@lido:pref, 'alternat')) or (not(@lido:pref) and @lido:addedSearchTerm='yes'))]">
  					<skos:altLabel>
  						<xsl:if test="string-length( lido:langSelect($descLang,@xml:lang)) &gt; 0">
  							<xsl:attribute name="xml:lang">
  								<xsl:value-of select="lido:langSelect($descLang,@xml:lang)" />
  							</xsl:attribute>
  						</xsl:if>
  						<xsl:value-of select="." />
  					</skos:altLabel>
  				</xsl:for-each>
  				<xsl:for-each select="lido:conceptID[starts-with(@lido:pref, 'alternat')][starts-with(., 'http://') or starts-with(., 'https://')]">
  					<skos:closeMatch>
  						<xsl:attribute name="rdf:resource">
  							<xsl:value-of select="." />
  						</xsl:attribute>
  					</skos:closeMatch>
  				</xsl:for-each>
   			</skos:Concept>
  	</xsl:for-each>      
  	<!-- skos:Concept -->
  	
       <!-- ore:Aggregation -->        
      <ore:Aggregation>
        <xsl:attribute name="rdf:about">
          <xsl:for-each select="lido:administrativeMetadata/lido:recordWrap/lido:recordID">
            <xsl:if test="position() = 1">
              <xsl:value-of select="concat($ore_Aggregation, $dataProvider,'/',.)" />
            </xsl:if>
          </xsl:for-each>
        </xsl:attribute>
        
        <!-- edm:aggregatedCHO -->
         <edm:aggregatedCHO>
          <xsl:attribute name="rdf:resource">
          <xsl:for-each select="lido:administrativeMetadata/lido:recordWrap/lido:recordID">
              <xsl:if test="position() = 1">
              <xsl:value-of select="concat($edm_providedCHO, $dataProvider,'/',.)" />
              </xsl:if>
            </xsl:for-each>
          </xsl:attribute>
        </edm:aggregatedCHO>
        <!-- edm:aggregatedCHO -->
        
        <!-- edm:dataProvider : lido:recordSource -->
        <xsl:choose>
			<xsl:when test="lido:administrativeMetadata/lido:recordWrap/lido:recordSource[@lido:type='europeana:dataProvider']">
				<xsl:for-each select="lido:administrativeMetadata/lido:recordWrap/lido:recordSource[@lido:type='europeana:dataProvider']/lido:legalBodyName/lido:appellationValue">
				  <xsl:if test="position() = 1">
					<edm:dataProvider>
					  <xsl:value-of select="." />
					</edm:dataProvider>
				  </xsl:if>
				</xsl:for-each>
			</xsl:when>
			<xsl:otherwise>
				<xsl:for-each select="lido:administrativeMetadata/lido:recordWrap/lido:recordSource/lido:legalBodyName/lido:appellationValue">
				  <xsl:if test="position() = 1">
					<edm:dataProvider>
					  <xsl:value-of select="." />
					</edm:dataProvider>
				  </xsl:if>
				</xsl:for-each>
			</xsl:otherwise>
		</xsl:choose>
		<!-- edm:dataProvider -->
		

        <xsl:for-each select="lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/lido:resourceRepresentation[@lido:type='image_master' or not(@lido:type) ]">
        <xsl:if test="position() &gt; 1">
		<xsl:for-each select="lido:linkResource[starts-with(., 'http://') or starts-with(., 'https://') or starts-with(., 'ftp://')]">
			<edm:hasView>
				<xsl:attribute name="rdf:resource">
                  <xsl:value-of select="." />
				</xsl:attribute>
			</edm:hasView>
       </xsl:for-each>
       </xsl:if>
       </xsl:for-each>
       <!-- edm:hasView --> 

		

       
       <!-- edm:hasView : lido:resourceRepresentation / @lido:type=image_master or empty -->        
        <xsl:for-each select="lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/lido:resourceRepresentation[@lido:type='image_thumb']">
        <xsl:if test="position() &gt; 1">
		<xsl:for-each select="lido:linkResource[starts-with(., 'http://') or starts-with(., 'https://') or starts-with(., 'ftp://')]">
			<edm:hasView>
				<xsl:attribute name="rdf:resource">
                  <xsl:value-of select="." />
				</xsl:attribute>
			</edm:hasView>
       </xsl:for-each>
       </xsl:if>
       </xsl:for-each>
       <!-- edm:hasView --> 
		
		<!-- edm:isShownAt : lido:recordInfoLink -->
         <xsl:if test="lido:administrativeMetadata/lido:recordWrap/lido:recordInfoSet/lido:recordInfoLink[starts-with(., 'http://') or starts-with(., 'https://')]">
              <xsl:for-each select="lido:administrativeMetadata/lido:recordWrap/lido:recordInfoSet/lido:recordInfoLink">
                <edm:isShownAt>
                    <xsl:attribute name="rdf:resource">
                	<xsl:if test="position() = 1">
                  		<xsl:value-of select="." />
                	</xsl:if>
                    </xsl:attribute>
                </edm:isShownAt>
              </xsl:for-each>
          </xsl:if>
		<!-- edm:isShownAt -->

		<!-- edm:isShownBy : lido:resourceRepresentation / @lido:type=iiif_image_api or =image_master or empty -->        
		<xsl:choose>
			<xsl:when test="lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/lido:resourceRepresentation[@lido:type='iiif_image_api']">
        <xsl:for-each select="lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/lido:resourceRepresentation[@lido:type='iiif_image_api']">
        <xsl:if test="position() = 1">
		<xsl:for-each select="lido:linkResource[starts-with(., 'http://') or starts-with(., 'https://')]">
			<edm:isShownBy>
				<xsl:attribute name="rdf:resource">
                  <xsl:value-of select="concat(text(), '/full/full/0/default.jpg')" />
				</xsl:attribute>
			</edm:isShownBy>
       </xsl:for-each>
	   </xsl:if>
       </xsl:for-each>
			</xsl:when>
			<xsl:otherwise>
        <xsl:for-each select="lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/lido:resourceRepresentation[@lido:type='image_master' or not(@lido:type)]">
        <xsl:if test="position() = 1">
		<xsl:for-each select="lido:linkResource[starts-with(., 'http://') or starts-with(., 'https://') or starts-with(., 'ftp://')]">
			<edm:isShownBy>
				<xsl:attribute name="rdf:resource">
                  <xsl:value-of select="." />
				</xsl:attribute>
			</edm:isShownBy>
       </xsl:for-each>
	   </xsl:if>
       </xsl:for-each>
			</xsl:otherwise>
		</xsl:choose>
       <!-- edm:isShownBy -->          

       <!-- edm:object -->    
        <xsl:for-each select="lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/lido:resourceRepresentation[@lido:type='image_thumb']">
        <xsl:if test="position() = 1">
        <xsl:for-each select="lido:linkResource[starts-with(., 'http://') or starts-with(., 'https://') ]">
          
			<edm:object>
				<xsl:attribute name="rdf:resource">
                  <xsl:value-of select="." />
				</xsl:attribute>
			</edm:object>
          
       </xsl:for-each>
       </xsl:if>
       </xsl:for-each>
       <!-- edm:object -->    

       <!-- edm:provider -->    
       <edm:provider>
          <xsl:value-of select="$edm_provider" />
       </edm:provider>
       <!-- edm:provider -->
               
       <!-- edm:rights : lido:rightsResource (MANDATORY, URI taken from a  set of URIs defined for use in Europeana) -->    
      	<xsl:for-each select="lido:administrativeMetadata/lido:resourceWrap/lido:resourceSet/lido:rightsResource/lido:rightsType[lido:conceptID[(starts-with(., 'http://') or starts-with(., 'https://')) and (contains(., 'creativecommons.org/') or contains(., 'www.europeana.eu/rights') or contains(., 'rightsstatements.org/'))] or lido:term[(starts-with(., 'http://') or starts-with(., 'https://')) and (contains(., 'creativecommons.org/') or contains(., 'www.europeana.eu/rights') or contains(., 'rightsstatements.org/'))]]">
          <xsl:if test="position() = 1">
            <edm:rights>
            	<xsl:attribute name="rdf:resource">
					<xsl:choose>
						<xsl:when test="lido:conceptID[(starts-with(., 'http://') or starts-with(., 'https://')) and (contains(., 'creativecommons.org/') or contains(., 'www.europeana.eu/rights') or contains(., 'rightsstatements.org/'))]">
							<xsl:value-of select="lido:conceptID[contains(., 'creativecommons.org/') or contains(., 'www.europeana.eu/rights') or contains(., 'rightsstatements.org/')][1]/text()" />
						</xsl:when>
						<xsl:when test="lido:term[(starts-with(., 'http://') or starts-with(., 'https://')) and (contains(., 'creativecommons.org/') or contains(., 'www.europeana.eu/rights') or contains(., 'rightsstatements.org/'))]">
							<xsl:value-of select="lido:term[contains(., 'creativecommons.org/') or contains(., 'www.europeana.eu/rights') or contains(., 'rightsstatements.org/')][1]/text()" />
						</xsl:when>
						
					</xsl:choose>
		        </xsl:attribute>
            </edm:rights>
          </xsl:if>
        </xsl:for-each>
        <!-- edm:rights -->  
        
        	  <!-- -->        
        <xsl:for-each select="lido:administrativeMetadata/lido:recordWrap/lido:recordSource[@lido:type='europeana:intermediateProvider']/lido:legalBodyName/lido:appellationValue">        
        <xsl:if test="position() &gt;= 1">
			<edm:intermediateProvider>
				<xsl:value-of select="." />
			</edm:intermediateProvider>
       </xsl:if>
       </xsl:for-each>
       <!-- --> 
        
      </ore:Aggregation> 
	  <!-- ore:Aggregation -->   
    </xsl:for-each>
  </rdf:RDF>
 </xsl:template>
 
</xsl:stylesheet>
