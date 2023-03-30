package gr.ntua.ivml.mint.api.handlers;

import static gr.ntua.ivml.mint.api.RequestHandlerUtil.getUniqueParameter;
import static gr.ntua.ivml.mint.api.RequestHandlerUtil.grab;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import org.apache.commons.io.FileUtils;

import gr.ntua.ivml.mint.api.RequestHandlerUtil;
import gr.ntua.ivml.mint.api.RouterServlet.Handler;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.AccessAuthenticationLogic;
import gr.ntua.ivml.mint.persistent.AnnotationSet;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.User;

public class AnnotationSetHandlers {
	public static Map<String, Handler> handlers() {
		Map<String, Handler> result = new HashMap<>();
		result.put( "GET /annotationSet", get());
		result.put( "POST /annotationSet", create());
		return result;
	}
	
	static class TmpFile  implements Closeable {
		public File file;
		public TmpFile( ) throws IOException {
			this.file = File.createTempFile("annotation", ".part");
		}
		
		public void close() {
			if( file != null )
				file.delete();
		}
	}
	
	enum UploadFormat {
		JSON_LD_TGZ, JSON_LD_GZ, CSV_GZ_WITH_HEADERS, CSV_GZ_NO_HEADERS
		// and whatever else we can come up...
	}

	private static boolean  isMultipart( HttpServletRequest request ) throws IOException {
		return request.getContentType().toLowerCase().contains("multipart/form-data");
	}
	
	// get handler
	private static Handler get() {
		// grab throws Parameter exception, errorWrap catches that and make json 404  response out of it.
		
		Handler result =  (request, repsone ) -> {
			User user = grab( ()-> (User) request.getAttribute("user"), "Login is required"); 
		};
		return RequestHandlerUtil.errorWrap(result);
	}
	
	private static void createFromMultipart( HttpServletRequest request,  HttpServletResponse response ) throws IOException {
		
		AnnotationSet result = new AnnotationSet();
		
		User user = grab( ()-> (User) request.getAttribute("user"), "Login is required"); 

		Part file = grab( ()->request.getPart("file"), "'file' missing in Multipart upload") ;
		result.setOrganization( user.getOrganization());
		Optional<String> orgIdOpt = getUniqueParameter(request, "orgId");
		if( orgIdOpt.isPresent()) {
			String s = orgIdOpt.get();
			long orgId = grab( ()->Integer.parseInt(s), "orgId is not integer.");
			result.setOrganization(
					grab( ()-> DB.getOrganizationDAO().getById( orgId, false), "Organization " + orgId+ "is not in DB")
			);
		};
		
		RequestHandlerUtil.check( ()-> AccessAuthenticationLogic.canUserWrite(user, result.getOrganization()), "User has no right to do that");
		
		Optional<Dataset> targetDatasetOpt = RequestHandlerUtil
				.getUniqueNumberParameter(request, "targetDatasetId") //Parameter exception if not a number
				.map( l-> grab(()-> DB.getDatasetDAO().getById( l, false ), "Dataset " + l + " not in DB")); //parameter if dataset doesnt exist
		
		
		targetDatasetOpt.ifPresent( ds ->RequestHandlerUtil.check( 
				()-> AccessAuthenticationLogic.canUserRead(user, ds ), "User has no right to target Dataset"));
		
		
		Optional<Dataset> originalTargetDatasetOpt = RequestHandlerUtil
				.getUniqueNumberParameter(request, "targetOriginalDatasetId")
				.map( l-> grab(()-> DB.getDatasetDAO().getById( l, false ), "Dataset " + l + " not in DB"));

		originalTargetDatasetOpt.ifPresent( ds ->RequestHandlerUtil.check( 
				()-> AccessAuthenticationLogic.canUserRead(user, ds ), "User has no right to target Dataset"));

		// format, optional, default is json_ld_tgz
		UploadFormat format = getUniqueParameter(request, "format").map( s-> UploadFormat.valueOf(s)).orElse( UploadFormat.JSON_LD_TGZ);
		
		
		try( TmpFile tmpFile = new TmpFile()) {
			FileUtils.copyInputStreamToFile(file.getInputStream(),tmpFile.file );
		} finally {}
		
	}
	
	// I guess 2 ways would be good,
	// multipart request with file-data and metadata json
	// 
	private static Handler create() {
		Handler result =  (request, response ) -> {
			if( isMultipart(request)) {
				createFromMultipart( request, response );
				return;
			}
		};
			
			/*
			Part name = request.getPart("name");
			//if( name==null ) return "No 'name' in multipart-request" ;

			try( TmpFile tmp = 	new TmpFile( File.createTempFile("enrich", ".csv"))) {
	
				FileUtils.copyInputStreamToFile(file.getInputStream(),tmp.file );
				String enrichmentName = IOUtils.toString(name.getInputStream(), StandardCharsets.UTF_8);

				Enrichment enrichment = new Enrichment();
				enrichment.initialize(tmp.file, enrichmentName, u);
				
				String orgIdString = request.getParameter("orgId");
				if(! StringUtils.empty(orgIdString)) {
					long orgId = Long.parseLong(orgIdString);
					Organization org = DB.getOrganizationDAO().getById(orgId, false);
					if( org == null ) return "orgId was specified but doesn't exist";
					
					boolean access = false;
					if( u.isAccessibleOrganization(org)) access = true;
					if( !Project.sharedIds(u.getProjectIds(),org.getProjectIds()).isEmpty()) access = true;
					if( !access ) return "Cannot access given Organization.";
					
					enrichment.setOrganization(org);
					
					// if user is project user, set projects on enrichment 
					enrichment.setProjectIds( Project.sharedIds(u.getProjectIds(), org.getProjectIds()));
				}
				
				DB.getEnrichmentDAO().makePersistent(enrichment);
				DB.commit();

				// prepare success response
				ObjectMapper om = new ObjectMapper();

				ObjectNode node = om.createObjectNode();

				node.put("enrichmentId", enrichment.getDbID());
				node.put("enrichmentName", enrichment.getName());
				node.put("lineCount", enrichment.getLineCount());
				node.put("creator", enrichment.getCreator().getDbID());
				node.put("organization", enrichment.getOrganization().getDbID());
				node.set("projectIds", om.valueToTree( enrichment.getProjectIds()));
				okJson(node, response);
				return null;
			} // clean up tmp file
		};

		try { 
			String error = exec.get();
			if( error != null ) errJson( error, response );
		} catch( Exception e ) {
			log.error( "Exception during Enrich creation" , e );
			errJson( e.getMessage(), response );	
		}		

		};
		*/
		return RequestHandlerUtil.errorWrap(result);
		
	}
}
