#!/usr/bin/env groovy
@GrabConfig(systemClassLoader=true) 
@Grapes([
        @Grab(group='io.rest-assured', module='rest-assured', version='4.4.0'),
        @GrabExclude('org.codehaus.groovy:groovy-xml')
])

import io.restassured.RestAssured
import io.restassured.filter.cookie.CookieFilter
import io.restassured.http.ContentType;

import static io.restassured.RestAssured.given

def cli = new CliBuilder( usage:'MintUploadEnrichment [options] csv-filename')

cli.header = "Upload a csv file (with header line) to given Mint instance and organization id."
cli.m( 'mint url', args:1, longOpt:'mint' )
cli.u( 'username:password', required:true, args:1)
cli.o( 'organization id', required:true, args:1 )
 

def options = cli.parse(args)

if( options == null ) return

if( ! options.arguments() ) {
	println( "Need a file with csv data")
	return
}

loginFilter = new CookieFilter()
RestAssured.baseURI = options.m;
String[] userPass =options.u.split(":")

login( loginFilter, userPass[0], userPass[1] )
upload( loginFilter, new File(options.arguments()[0] ), options.o )


// login to mint ideally as superuser
def login( loginFilter, user, pass ) {
    given()
            .param("username", user)
            .param("password", pass)
            .filter(loginFilter)
            .when().post("/api/login");
}

def upload( loginFilter, File f, String orgId ) {
    int enrichmentId = given()
        .multiPart("file", f)
        .multiPart( "name", f.name  )
        .param( "orgId", orgId )
        .filter( loginFilter )
        .when().post("/api/enrich")
        .then()
        .extract().path( "enrichmentId" )
    return enrichmentId
}
