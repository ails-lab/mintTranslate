import gr.ntua.ivml.mint.concurrent.Queues
import gr.ntua.ivml.mint.db.DB
import gr.ntua.ivml.mint.persistent.Item
import gr.ntua.ivml.mint.util.ApplyI
import gr.ntua.ivml.mint.xml.util.XmlValueUtils
import groovy.json.*
import nu.xom.*

// Duration fix EUscreen
// modify items in a selected subset of datasets
// according to a json file supplied from /tmp

def json = new JsonSlurper().parse( new File( "/tmp/duration.json"))

durations = [:]
for( def dur: json ) {
	durations[dur["id"]]=dur["duration"]
}


def docModifier(Document doc) {
	// check if the EDM is there
	Optional<String> optId = XmlValueUtils.getUniqueValue(doc, "//*[local-name() = 'ProvidedCHO']/@*[local-name()='about']");
	if( optId.isPresent()) {
		String id = optId.get().replaceAll( ".*/([^/]+)\$","\$1")
		String dur = durations[id]
		if( dur == null ) return false
		def extentList = XmlValueUtils.queryDoc(doc, "//*[local-name() = 'ProvidedCHO']/*[local-name()='extent']");
		for( Node n: extentList ) {
			if( n.getValue().matches( "(.*:){2}.*" )) {
				((Element)n).removeChildren()
				((Element)n).insertChild(dur, 0)
				return true
			}
		}
		return false
	}
	// else
	optId = XmlValueUtils.getUniqueValue(doc, "//*[local-name() = 'AdministrativeMetadata']/*[local-name()='identifier']")
	if( optId.isPresent()) {
		String id = optId.get()
		String dur = durations[id]
		if( dur == null ) return false
		def durationList = XmlValueUtils.queryDoc(doc, "//*[local-name() = 'TechnicalInformation']/*[local-name()='itemDuration']");
		for( Node n: durationList ) {
			if( n.getValue().matches( "(.*:){2}.*" )) {
				((Element)n).removeChildren()
				((Element)n).insertChild(dur, 0)
				return true
			}
		}
	}
	return false
}

def counter = [0,0]

ApplyI<Item> itemModifier = {
	Item item ->
	Document doc;
	counter[1]++
	if( counter[1]%1000 == 0 ) log.info( "Processed " + counter[1] )
	try {
		doc = item.getDocument()
		if( doc == null ) return
	} catch( Exception e ) {
		// didnt parse, dont care
		return
	}
//	log.info( "Item "+item.getDbID())
	String oldXml = doc.toXML()
	if( docModifier(doc)) {
//		String newXml = doc.toXML()
//		println( "Old XML \n" + oldXml)
//		println( "\n\nNew XML \n" + newXml)
		counter[0]++
//		if( counter[0] >5 ) throw new Exception( "Modified enough items")
		if( counter[0]%100 == 0 ) log.info( "Changed " + counter[0] + " items.")
	}
}


// only datasets that have been in shape for portal or derived can be affected,
// find all dataset ids that are theoretical possible
// build the trees for all datasets
// check which trees can be omitted 

def l = DB.getSession()
.createQuery("select ds, s.name from Dataset ds left join ds.schema s")
//.setMaxResults(30)
.list()

def dsLookup = [:]
for( def rec: l ) {
   dsLookup[rec[0].dbID] = [ "id":rec[0].dbID, "parent":rec[0].parentDataset?.dbID, 
       "itemCount":rec[0].itemCount, "type":rec[0].class, "schema":rec[1]]
}

// this should build the upload tree

for( def entry: dsLookup.values()) {
   if( entry.parent != null ) {
       def rec = dsLookup[entry.parent]
	   def children = rec.children
	   if( children == null ) {
		   children = []
		   rec.children = children
	   }
	   children.add( entry )
   }
}

def allSubsets( def ds, def result ) {
	result.add( ds.id)
	for( def child: ds.children ) 
		allSubsets( child,result)
}

def allSubsets( def ds ) {
	def res = [] as Set
	allSubsets( ds, res )
	return res	
}

def hasPortalSet( def dss, Set portalDs ) {
	for( def ds: dss )
		if( portalDs.contains(ds)) return true
	return false;
}

def portalSchemas = [ "EUScreen_valid","EUscreen OLD","EUscreenXL-Core",
	"EUscreen ITEM/CLIP", "EUscreenXL ITEM/CLIP v2" ]

def portalSets = dsLookup.values().findAll { portalSchemas.contains( it.schema )}.collect{ it.id } as Set


dsLookup.values().each{ it.allSets = allSubsets( it ) }
println( dsLookup.size())
def trees = dsLookup.findAll{ it.value.parent == null }
// filter trees
// println( trees.size())
trees = trees.findAll{ hasPortalSet( it.value.allSets, portalSets )}
// println( trees.size())

def dssIds = trees.values().collectMany{ it.allSets }


DB.SessionRunnable  r= {
	for( dsId in dssIds ) {
		def ds = DB.datasetDAO.getById( dsId, false)
		if (ds.schema != null ) {
			ds.processAllItems( itemModifier, false )
		}
	}
}

Queues.queue( r, "now")
