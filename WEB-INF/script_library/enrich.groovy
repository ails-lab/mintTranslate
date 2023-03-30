// Script for mass enrichment
import gr.ntua.ivml.mint.persistent.*
import gr.ntua.ivml.mint.util.*
import gr.ntua.ivml.mint.f.*
import nu.xom.*
import groovy.transform.*

// Compilestatic to avoid the modifier having issues midflight
// edit the enrichmets you want to apply, the key generation in the mod method
// the activity you want to do when enriching
// and the datasets you want to enrich .. they need to be in edm, mind you

@CompileStatic
def makeModifier() {
 
   Enrichment.EnrichBuilder titleEnrich = DB.enrichmentDAO.getById( 1082l, false ).getEnrichBuilder( 0 )
   Enrichment.EnrichBuilder creatorEnrich = DB.enrichmentDAO.getById( 1078l, false ).getEnrichBuilder( 0 ) 
   Enrichment.EnrichBuilder descrEnrich = DB.enrichmentDAO.getById( 1079l, false ).getEnrichBuilder( 0 )
   Enrichment.EnrichBuilder relatedEnrich = DB.enrichmentDAO.getById( 1080l, false ).getEnrichBuilder( 0 ) 
  
   def mod = { Document doc -> 
        Nodes nodes = doc.query( "//*[local-name()='ProvidedCHO']/@*[local-name()='about']");
        if( nodes.size() != 1 ) return;
        String key = nodes.get(0).getValue();
        key =  key.replaceAll( ".*(europeana-fashion/.+)\$",'\$1' )
        key = org.apache.commons.lang.StringUtils.replaceChars(key, " /.-+?:&=()","___________");
        key = "http://data.europeana.eu/proxy/provider/2048221/" + key

        titleEnrich.enrich( key, { String[] row -> EdmEnrichBuilder.enrichTitle(  doc, "en", row[1], row[2] )})
        descrEnrich.enrich( key, { String[] row -> EdmEnrichBuilder.enrichDescription( doc, "en", row[1], row[2] )})
        creatorEnrich.enrich( key, {String[] row -> EdmEnrichBuilder.enrichCreator( doc, row[1], row[3] )})
        relatedEnrich.enrich( key, {String[] row -> EdmEnrichBuilder.enrichRelatedTo( doc, row[1], row[3] )})

        EdmEnrichBuilder.removeDuplicateRdfResource( doc )
        EdmEnrichBuilder.tagXX( doc, true )
   } as ThrowingConsumer<Document>
   return mod
} 
   
def interceptor = Interceptor.modifyInterceptor( makeModifier() )

def schema = DB.xmlSchemaDAO.simpleGet( "name = 'EDM annotated'")

new EdmEnrichBuilder().setName( "Enrich and tag" )
.setTargetSchema( schema  )
.setDocumentInterceptor( interceptor )

//.submit( 8498, 8276, 8261 )
