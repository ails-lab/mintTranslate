@Grab(group='org.apache.httpcomponents.client5', module='httpclient5', version='5.1.3')
@Grab(group='org.apache.httpcomponents.client5', module='httpclient5-fluent', version='5.1.3')
// @Grab(group='org.codehaus.groovy', module='groovy-xml', version='2.5.14')


import groovy.json.*
import org.apache.hc.client5.http.fluent.Request
import java.util.regex.*
import java.util.concurrent.*
import java.util.concurrent.atomic.*
import java.nio.charset.Charset
import groovy.xml.*


def allResults = [] as LinkedBlockingQueue
def threads = []

// get all the euscreen users on the Noterik server
for( user in userList() ) {
	// sync download, async process, but process os so fast, makes almost no sense
	threads.add( downloadOrg( user, allResults ))
}

threads.forEach{ it.join()}

// writing some json out

String filename = "noterikVideos.json"

def jsonOut = JsonOutput.prettyPrint( 
	JsonOutput.toJson( allResults )
)
new File( filename ).withWriter( "UTF-8") {
	w -> w.println( jsonOut )
}

def userList( ) {
	def parser = new XmlParser()
	def noterickApproved = "http://euscreen.eu:8080/bart/domain/euscreenxl/user/"
	def providersXml = getXML( noterickApproved, parser )
	return providersXml."**".findAll{ n-> n.name() == "user" && n.@id.startsWith( "eu_")}*.@id
}

def getXML( String url, parser ) {
        return parser.parseText(
        	Request.get( url )
            .execute()
	    .returnContent()
	    .asString( Charset.forName( "UTF-8")));
}

def extractInfo( node, orgname ) {
	def meta = [:]
	meta["id"] = node.@id
	meta["eu_provider"] = orgname
	meta["type"] = node.name()
	def isPublic = getField( node, "public").orElse( "false") != "false"

	meta["isPublic"] = isPublic
	if( isPublic ) {
		getField( node, "mount").ifPresent{ s -> meta["documentUrl"] = s };
		getField( node,"screenshot").ifPresent{ s-> meta["screenShot"] = s }
		getField( node, "provider" ).ifPresent{ s-> meta["provider"] = s }
	}
	return meta
}


def downloadOrg( orgname, results ) {
	def nodeSet = ["series", "picture", "doc", "video", "audio" ] as Set
	String url = "http://euscreen.eu:8080/smithers2/domain/euscreenxl/user/" + orgname 
	println( "Download $orgname")
	String xml = Request.get( url )
        .execute()
	    .returnContent()
	    .asString( Charset.forName( "UTF-8"))

	return Thread.start{
		println( "Parsing $orgname")
		def parser = new XmlParser()
		def doc = parser.parseText( xml )
		def nodes = doc."**".findAll{ node -> 
			nodeSet.contains( node.name()) 
		}.findAll{ 
			node -> node.@referid == null 
		}

		for( node in nodes ) {
			results.add( extractInfo( node, orgname))
		}
		println( "Finished $orgname")
	}
}

def getField( doc, field ) {
	if( doc."**".findAll{ n -> n.name() == field}.isEmpty()) return Optional.empty();
	return Optional.of( noterikUnescape(doc."**".find{ n-> n.name() ==field}.text()))
}


// daniels handmade XML quoting ... but why :-)
def noterikUnescape( String input ) {
    Pattern p = Pattern.compile( /\\\d{3}/ )
	StringBuffer res = new StringBuffer()
	Matcher m = p.matcher( input )
	while( m.find()) {
		String numS = m.group().substring(1)
		int num = Integer.parseInt(numS)
		m.appendReplacement(res, new String( Character.toChars(num )))
	}
	m.appendTail( res )
	return res.toString()
}

