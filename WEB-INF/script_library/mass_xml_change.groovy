// change the edm:rights in all XML files 

import gr.ntua.ivml.mint.persistent.*
import gr.ntua.ivml.mint.util.*
import gr.ntua.ivml.mint.mapping.model.*
import gr.ntua.ivml.mint.concurrent.*

count = -1;

change = [
"http://creativecommons.org/licenses/by-nc-nd/3.0/": "http://creativecommons.org/licenses/by-nc-nd/4.0/",
"http://www.europeana.eu/rights/rr-f/":"http://rightsstatements.org/vocab/InC/1.0/ ",
"http://creativecommons.org/licenses/by-sa/3.0/":"http://creativecommons.org/licenses/by-sa/4.0/",
"http://creativecommons.org/licenses/by-nc/3.0/":"http://creativecommons.org/licenses/by-nc/4.0/",
"http://creativecommons.org/licenses/by-nc-sa/3.0/":"http://creativecommons.org/licenses/by-nc-sa/4.0/",
"http://creativecommons.org/licenses/publicdomain/zero/":"http://creativecommons.org/publicdomain/zero/1.0/",
"http://creativecommons.org/licenses/by/3.0/":"http://creativecommons.org/licenses/by/4.0/"
]

ApplyI<Item> collectRights = new ApplyI<Item>() {
	public void apply( Item i) {
		if( count ==0 ) return
		try {
		nodes =  i.getNodes( "//*[local-name()='rights']/@*[local-name()='resource']" )
		boolean changeRight = false;
		for (int j = 0; j < nodes.size(); j++) {
			String value = nodes.get(j).getValue();
			if( change.containsKey( value )) {
				nodes.get(j).setValue( change[ value ] )
				changeRight = true
				log.info( "Changed #$i.dbID '$value' to '${change[value]}'")
			}
		}
		if( changeRight ) i.setXml( i.getDocument().toXML())
		// log.info( i.getDocument().toXML())
		} catch( Exception e ) {
			log.error( "Item #$i.dbID trouble", e )
		} finally {
			count--
		}
	}
}

task = {
	dss = DB.datasetDAO.findAll()
	for( ds in dss ) {
		if( count ==0 ) break
		if (ds?.schema?.name =~ /EDM/ ) {
			// items += ds.getItemCount()
			ds.processAllItems( collectRights, true )
		}
	}
}

Queues.queue( task, "now")
