import gr.ntua.ivml.mint.persistent.Item
import gr.ntua.ivml.mint.persistent.Dataset
import gr.ntua.ivml.mint.util.ApplyI
import gr.ntua.ivml.mint.xml.util.XmlValueUtils
import groovy.json.*
import nu.xom.*
import gr.ntua.ivml.mint.db.DB

// duration fix related code

/*
 * This is just a collection of code related to the activity, dont run this file
 */

def json = new JsonSlurper().parse( new File( "/tmp/duration.json"))

// find datasets that need republishing
// all core datasets


Map portalDsOrgLookup = DB.getPublicationRecordDAO().simpleList("target='euscreen.portal'").collectEntries{ [ it.publishedDataset.dbID, it.organization.dbID ] }
Map itemLookup = [:]
Map dsOriginLookup = [:]

// all core ds and item ids
// lookup original ds from itemId
for( def pair:portalDsOrgLookup ) {
	def ds = DB.datasetDAO.getById( pair.key, false)
	def originalId = ds.origin.dbID
	ds.processAllItems( {
		Item item ->
		if( item.persistentId =~ /^EUS_/ ) itemLookup[ item.persistentId ] = originalId
		else itemLookup[ item.dbID ] = originalId
	}, false);
}

// all original dsIds with modified items
Map dsItemCount= [:]
for( Map dur: json ) {
	def dsId = itemLookup[dur["id"]]
	if( dsId != null ) {
		int count = dsItemCount.computeIfAbsent(dsId, {k->0})
		dsItemCount[dsId] = count+1
	} else {
		println( "Item id ${dur.id} not found in datasets")
	}
}

class Help {
	String org
	String dsName
	String tags
	long id
}

// orgids and item counts, ds counts
Map orgItemDsCounts = [:]
List<Help> res = []

for( def pair: dsItemCount ) {
	Dataset ds = DB.datasetDAO.getById( pair.key, false )
	Help h = new Help()
	h.id = ds.dbID
	h.org = ds.organization.englishName
	h.tags = ds.getJsonFolders()
	h.dsName = ds.name;
	res.add( h )
	def orgId = ds.organization.dbID
	def counts = orgItemDsCounts.computeIfAbsent(orgId, {k->[0,0]})
	counts[0] += ds.validItemCount
	counts[1] += 1
}

res.sort { h1, h2 -> h1.org <=> h2.org ?: h1.tags <=> h2.tags ?: h1.dsName <=> h2.dsName }
for( Help h: res ) {
	println( "${h.org}\t${h.dsName},\t${h.tags}\t${h.id}")
}


Map edmDsOriginalLookup = DB.getPublicationRecordDAO().simpleList("target='euscreen.oai'")
  .collectEntries{ [ it.originalDataset.dbID,  it.publishedDataset.dbID ] }
  
Set republishIds = [] as Set
// affected edm publications
for( def pair: edmDsOriginalLookup)  {
	if( dsItemCount.containsKey( pair.key))
		republishIds.add( pair.value )
}


// {8919=35, 8917=124, 4833=25, 6097=20, 5615=26, 4869=11, 4628=4, 8587=16, 7438=8, 8092=8, 4952=8, 8909=11, 4630=10, 6094=18,
// 5917=16, 4929=10, 8918=16, 8912=8, 9784=3, 6629=25, 8910=2, 8913=6, 4671=1, 14467=3, 14777=1, 14616=15, 14608=22, 14617=13,
// 14512=4, 14610=29, 15362=6, 14613=17, 14621=26, 14614=19, 14618=25, 14609=20, 14612=29, 14622=23, 14611=10, 14619=22,
// 14615=17, 14623=17, 14620=22, 15717=3, 15321=322, 15310=184, 15328=24, 15323=31, 15306=124, 15311=67, 15326=12, 15334=28,
// 15315=15, 15317=15, 16086=7, 16178=17, 16180=2, 16164=98, 16176=3, 16160=5, 16168=4, 16184=2, 16170=2, 16158=3, 16186=1,
// 9759=24, 9770=18, 4981=17, 4984=15, 9772=13, 4484=17, 9774=15, 9775=20, 5470=14, 9777=15, 5496=13, 5022=10, 9776=21, 5700=20,
// 8468=4, 9771=2, 7477=26, 4775=23, 4674=25, 4583=1, 6305=14, 9761=6, 4412=12, 6749=8, 15831=6, 4353=1, 4345=1, 15330=14,
// 15322=46, 15318=23, 15329=21, 15324=17, 15336=9, 4957=1, 4426=1, 4386=1, 4480=10, 4824=1, 5103=1, 15331=13, 4878=1, 4533=1,
// 4558=1, 15325=11, 4592=1, 4973=1, 4728=1, 16188=1, 4448=1, 5104=1, 4643=1, 4762=1, 4692=1, 4358=1, 15319=31, 15316=22,
// 15333=4, 4492=1, 9092=150, 9778=1, 9058=273, 9093=202, 14468=1, 9081=154, 4343=1, 7637=98, 9108=144, 7636=127, 4940=1,
// 9106=128, 8137=208}

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
		if( dur == null ) return
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
		if( dur == null ) return
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
		String newXml = doc.toXML()
		item.setXml(newXml)
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

def portalSchemas = [ "EUScreen_valid","EUscreen OLD","EUscreenXL-Core",
				"EUscreen ITEM/CLIP", "EUscreenXL ITEM/CLIP v2" ]

def portalSets = dsLookup.values().findAll { portalSchemas.contains( it.schema )}.collect{ it.id } as Set


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

dsLookup.values().each{ it.allSets = allSubsets( it ) }
println( dsLookup.size())
def trees = dsLookup.findAll{ it.value.parent == null }
// filter trees
// println( trees.size())
trees = trees.findAll{ hasPortalSet( it.value.allSets, portalSets )}
// println( trees.size())

def dssIds = trees.values().collectMany{ it.allSets }

int items =0
for( dsId in dssIds ) {
	def ds = DB.datasetDAO.getById( dsId, false)
	if (ds.schema != null ) {
		items += ds.itemCount
		// ds.processAllItems( itemModifier, false )
	}
}

items
