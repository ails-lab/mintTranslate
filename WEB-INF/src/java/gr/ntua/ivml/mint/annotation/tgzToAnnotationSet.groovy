@Grab(group='xom', module='xom', version='1.2.5')
@Grab(group='com.fasterxml.jackson.core', module='jackson-databind', version='2.8.8')
@Grab(group='io.rest-assured', module='rest-assured', version='5.0.1')
@Grab(group='org.apache.httpcomponents', module='fluent-hc', version='4.5.13')
@Grab(group='org.apache.logging.log4j', module='log4j-core', version='2.19.0')
@Grab(group='org.apache.logging.log4j', module='log4j-api', version='2.19.0')
@Grab(group='commons-io', module='commons-io', version='2.11.0')
@Grab(group='org.apache.commons', module='commons-lang3', version='3.8.1')
@GrabExclude(group='org.apache.groovy', module='groovy-xml', version='4.0.1')
@Grab(group='org.apache.commons', module='commons-compress', version='1.22')

@GrabExclude( group='xml-apis', module='xml-apis')

@GrabConfig(systemClassLoader = true)

import io.restassured.RestAssured;
import io.restassured.http.ContentType;
import io.restassured.http.Cookies;
import io.restassured.http.*;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.node.*;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.util.concurrent.*

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.archivers.tar.TarArchiveOutputStream;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorOutputStream;
import org.apache.commons.io.IOUtils;

import nu.xom.Document;
import nu.xom.Builder;
import nu.xom.*

import groovy.json.*
import org.xml.sax.XMLReader;


import gr.ntua.ivml.mint.annotation.*;
import gr.ntua.ivml.mint.annotation.EdmLiteralsAsAnnotations.Literal;

// 
// groovy -cp ~/git/mint18/WEB-INF/classes tgzToAnnotationSet.groovy

def cli = new CliBuilder( usage:'tgzToAnnotationSet [2 letter lang code eg fr] [tgz file with edm xml]')

cli.header = """Talk to Pangeanic API and make an annotation set with translation
of fields that have given language tag into english"""

cli.h( "Usage", longOpt:'help' )

def options = cli.parse(args)
if( options == null ) System.exit()
if( options.h || options.arguments().length != 2 ) {
    cli.usage()
    System.exit(0)
}


// the fields you want to translate
String[] fieldList = new String[] {
    "title" ,
    "description" ,
    "provenance",
    "type" ,
    "subject" ,
    "medium" ,
    "format" ,
    "coverage",
    "temporal" ,
    "alternative" ,
    "spatial" 
}

public class AllDataGraph {
    // record Id index into ValueNodes
    private Map recordNodes = [:]

    // value index into ValueNodes
    private Map valueNodes = [:]

    public static class ValueNode  {
        public String value //key .. one value node per value

        // fieldname->aggregator->set of recordIds
        public Map recordSetByField  = [:]

        ValueNode( String recordId, String fieldName, String value ) {
            this.value = value
            add(recordId, fieldName )
        }

        public void add( String recordId, String fieldName ) {
            recordSetByField
                .computeIfAbsent( fieldName, {j->[] as Set})
                .add( recordId )
        }

    }

    public Set<ValueNode> getValueNodes() {
        return valueNodes.valueSet()
    }

    public ValueNode getNodeByLiteral( String val) {
        return valueNodes.get( val );
    }

    public synchronized void collectValue( String recordId,  String fieldName,String value ) {
        ValueNode current = valueNodes.computeIfAbsent( value, {
            j-> new ValueNode(recordId, fieldName, value )
        })
        current.add( recordId, fieldName )

        def valueNodeList = recordNodes.computeIfAbsent( recordId, {j->new ArrayList()})
        valueNodeList.add( current )
    }

}


def collectDoc( AllDataGraph allData, Document doc, String[] fieldList ) {

   List<Literal> res = EdmLiteralsAsAnnotations.edmExtract( doc, fieldList )
    for( Literal l: res ) {
        def hash = l.asMap()
        if( !hash.containsKey( "language")) continue
        if( hash.field.startsWith( "skos:")) continue
        if( hash.language == "en") continue
        if( hash.language == "ita" ) hash.language = "it"
        if( hash.language == "dut") hash.language = "nl"

        // check only for the entries that can be translated
        if( hash.language != lang ) continue;
        if( l.value.length() > 2000 ) {
            println( "There is a limit of 2000 characters on field length. ")
            println( "Discarded ${hash.recordId} Field ${hash.field} Length: ${hash.value.length()}")
            continue
        }

        allData.collectValue( hash.recordId, hash.field, hash.value )
    }
}
 
 // groovy -cp /Users/arne/eclipse-workspace/AnnotationSet/bin/main/

def processFile( File f, Closure docProcess ) {
    try {
            if( ! f.name.endsWith( "tgz")) return

        	def fis = new FileInputStream( f );
			def bis = new BufferedInputStream(fis);
			def gzin = new GzipCompressorInputStream(bis);
			def tin = new TarArchiveInputStream(gzin );
            def fiis = new FilterInputStream( tin ) {
				public void close() throws IOException {};
			};

			TarArchiveEntry tae;
			while((tae=tin.getNextTarEntry()) != null) {
				if( tae.isFile()) {
                    try {
                        Document doc = Util.builder.build( fiis );
                        docProcess( f.absolutePath+":"+tae.name, doc )
                    } catch( Exception e ) {
                        e.printStackTrace()
                    }
                }
			}
			// all good, close up
			tin.close(); gzin.close();
			bis.close(); fis.close();

    } catch( Exception e ) {
        e.printStackTrace()
    } finally {

    }

}

def executor = Executors.newFixedThreadPool( 8 )

def allData = new AllDataGraph()
Closure c = {
    name, doc ->
    executor.execute( {
        collectDoc( allData, name, doc )
    } )
}

filename = args[1]
lang = args[0]
processFile( new File( filename ), c )

executor.shutdown()
executor.awaitTermination( 1, TimeUnit.MINUTES )

List<Literal> toTranslate = 
    allData.getValueNodes()
    .collect{ vn  -> 
        def l = new Literal() 
        l.field = "placeholder"
        l.value = vn.value
        l.language = lang
        l.recordId = "placeholder"
        l
    }

def annotationJson = EdmLiteralsAsAnnotations.createAnnotationSetFromLiterals( toTranslate, 5000 )
// now based on value we need to fill in scope and target source

def newAnnotations = Jackson.om().createArrayNode()
ObjectMapper om = Jackson.om()
for( ObjectNode ann: (ArrayNode) annotationJson.get( "@graph")) {
    // get the original literal out
    String literal = ann.at( "target.destination.value")
    ValueNode vn = allData.getNodeByLiteral( literal ) 
    if( vn == null ) continue
    for( Map.Entry entry in vn.recordSetByField ) {
        String fieldname = entry.key
        for( String recordId in entry.value ) {
            // set scope
            // target.source, target.selector.property
            ObjectNode annCopy = om.readTree(om.writeValueAsString(ann));
            annCopy.put( "scope", fieldname )
            annCopy.get( "target").put( "source", recordId )
            annCopy.at( "target.selector").put( "property", fieldname )
            newAnnotations.add( annCopy )
        }
    }
}    

// write annotations out

annotationJson.put( "@graph", newAnnotations )
Util.writeValueToUtf8File( annotationJson, filename.replaceAll( ".tgz", ".${lang}_en.json"))

class Util {


    // some stuff that we use
    static  ObjectMapper  om = new ObjectMapper();
    static XMLReader parser = javax.xml.parsers.SAXParserFactory.newInstance().newSAXParser().getXMLReader();
    static {
        om.enable(SerializationFeature.INDENT_OUTPUT);
        om = om.setSerializationInclusion(Include.NON_NULL)
        parser.setFeature( 'http://apache.org/xml/features/nonvalidating/load-external-dtd', false);
    }
        
    static Builder builder = new Builder(parser);

    static void dumpAsJson( Object obj ) {
        // def jsonNode = om.valueToTree( obj )
        println( om.writeValueAsString( obj ))
    }

    static void writeValueToUtf8File( Object obj, String filename ) {
        File f = new File( filename ) 
        f.write(  om.writeValueAsString( obj ), "UTF-8")
    }

    static void  count( Map m, Object k ) {
        count( m,k,1)
    }

    static void  count( Map m, Object k, int offset ) {
        int counter = m.computeIfAbsent( k, {j->0})
        m[k] = counter + offset
    }

    static void dumpCountersByAggregator( Map counters ) {
        Map res = [:]
        for( String key: counters.keySet()) {
            String[] parts = key.split("-")
            if(parts.length != 3) continue;
            int tmp = res.computeIfAbsent( parts[0], {j->[:]})
            .computeIfAbsent( parts[1], {j->0})
            res[parts[0]][parts[1]] = tmp + counters[key]
            tmp = res[parts[0]].computeIfAbsent( parts[2], {j->0})
            res[parts[0]][parts[2]] = tmp + counters[key]
        }
        dumpAsJson( res )
    }
}

