// change enumeration in mapping on all edm:rights
// Change mapping cases edm rights URLs

import gr.ntua.ivml.mint.persistent.*
import gr.ntua.ivml.mint.util.*
import gr.ntua.ivml.mint.concurrent.*
import gr.ntua.ivml.mint.mapping.model.*
import gr.ntua.ivml.mint.mapping.*

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.*

om = new ObjectMapper()

// all datasets with edm-fp / edm
// if transformation, check mapping
// collect edm:rights/rdf:resource
allRights = ["http://www.europeana.eu/rights/rr-f/",
"http://www.europeana.eu/rights/rr-p/",
"http://www.europeana.eu/rights/rr-r/",
"http://www.europeana.eu/rights/unknown/",
"http://www.europeana.eu/rights/out-of-copyright-non-commercial/" ,
"http://www.europeana.eu/rights/orphan-work-eu/" ,
"http://creativecommons.org/publicdomain/mark/1.0/",
"http://creativecommons.org/publicdomain/zero/1.0/",
"http://creativecommons.org/licenses/by/3.0/",
"http://creativecommons.org/licenses/by-sa/3.0/",
"http://creativecommons.org/licenses/by-nc/3.0/",
"http://creativecommons.org/licenses/by-nc-sa/3.0/",
"http://creativecommons.org/licenses/by-nd/3.0/",
"http://creativecommons.org/licenses/by-nc-nd/3.0/",

"http://creativecommons.org/licenses/by/4.0/" ,
"http://creativecommons.org/licenses/by-sa/4.0/" ,
"http://creativecommons.org/licenses/by-nc/4.0/" ,
"http://creativecommons.org/licenses/by-nc-sa/4.0/" ,
"http://creativecommons.org/licenses/by-nd/4.0/" ,
"http://creativecommons.org/licenses/by-nc-nd/4.0/" ,
"http://rightsstatements.org/vocab/NoC-OKLR/1.0/" ,
"http://rightsstatements.org/vocab/NoC-NC/1.0/" ,
"http://rightsstatements.org/vocab/InC/1.0/" ,
"http://rightsstatements.org/vocab/InC-EDU/1.0/" ,
"http://rightsstatements.org/vocab/InC-OW-EU/1.0/" ,
"http://rightsstatements.org/vocab/CNE/1.0/"
]



def swapUrls( String input ) {
	Map change = [
"http://creativecommons.org/licenses/by-nc-nd/3.0/": "http://creativecommons.org/licenses/by-nc-nd/4.0/",
"http://www.europeana.eu/rights/rr-f/":"http://rightsstatements.org/vocab/InC/1.0/ ",
"http://creativecommons.org/licenses/by-sa/3.0/":"http://creativecommons.org/licenses/by-sa/4.0/",
"http://creativecommons.org/licenses/by-nc/3.0/":"http://creativecommons.org/licenses/by-nc/4.0/",
"http://creativecommons.org/licenses/by-nc-sa/3.0/":"http://creativecommons.org/licenses/by-nc-sa/4.0/",
"http://creativecommons.org/licenses/publicdomain/zero/":"http://creativecommons.org/publicdomain/zero/1.0/",
"http://creativecommons.org/licenses/by/3.0/":"http://creativecommons.org/licenses/by/4.0/"
]

	output = input
	change.each { k,v ->
		output = output.replaceAll( k, v)
	}
	// println( "Input: $input => Output: $output")
	return output
}

// return modified json
def modifyMappingJson( String inputJson ) {
	mp = new Mappings( inputJson )
	elems = mp.find("//edm:rights/@rdf:resource")

	// modify enumeration
	for( elem in elems ) {
		elem.removeEnumerations()
		for( right in allRights )
			elem.addEnumeration( right )
		if( elem.hasMappingCases() ) {
			String mcjson = om.writeValueAsString( elem.getArray( JSONHandler.ELEMENT_CASES ))
			mcjson = swapUrls( mcjson )
			elem.setArray( JSONHandler.ELEMENT_CASES, JSONUtils.parseArray( mcjson ))
		}
	}
	return mp.toString()
}

def mappingFix = new ApplyI<Mapping>() {
	int max = -1
	public void apply( Mapping mapping ) {
	   if( max == 0 ) return
		try {
			if( !StringUtils.empty( mapping.jsonString )) {
				String newMappingJson = modifyMappingJson( mapping.jsonString );
				// log.info( "Original json $mapping.name")
				// log.info( mapping.jsonString )
				// log.info( "New json")
				// log.info( newMappingJson )
				log.info( "Fixing $mapping.name #$mapping.dbID.")
				mapping.setJsonString( newMappingJson )
			}  else {
				log.info( "No json in $mapping.name")
			}
		} catch( Exception e ) {
			log.error( "Problem with mapping $mapping.name", e )
		} finally {
			max--
		}
	}
}
 
checkThis = {
		DB.mappingDAO.onAll( mappingFix, null, false )
	}


Queues.queue( checkThis, "now")
