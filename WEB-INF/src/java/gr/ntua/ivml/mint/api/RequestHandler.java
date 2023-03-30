package gr.ntua.ivml.mint.api;

import java.io.Closeable;
/**
 * Request Handler class. Contains methods that serve requests.
 * Methods here are responsible for serving requests to /api/*.
 * The methods are reflected to RouterServlet and called from
 * there.
 *
 * Naming convention: {resource_name}+{HTTP verb}.
 * For example, to serve the GET request of /api/test, the method
 * MUST be named testGet in order to be correctly reflected by
 * the router.
 */
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;

import com.beust.jcommander.ParameterException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

import gr.ntua.ivml.mint.concurrent.Queues;
import gr.ntua.ivml.mint.concurrent.XSLTransform;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.f.ErrorCondition;
import gr.ntua.ivml.mint.f.GetOptions;
import gr.ntua.ivml.mint.f.ThrowingSupplier;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Enrichment;
import gr.ntua.ivml.mint.persistent.Lock;
import gr.ntua.ivml.mint.persistent.Organization;
import gr.ntua.ivml.mint.persistent.Project;
import gr.ntua.ivml.mint.persistent.Transformation;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.persistent.XmlSchema;
import gr.ntua.ivml.mint.persistent.XpathHolder;
import gr.ntua.ivml.mint.util.Config;
import gr.ntua.ivml.mint.util.Jackson;
import gr.ntua.ivml.mint.util.Label;
import gr.ntua.ivml.mint.util.StringUtils;


public class RequestHandler {

	public static Logger log = Logger.getLogger(RequestHandler.class);
	public static JsonNodeFactory json = JsonNodeFactory.instance;

	public static List<GetOptions> optionsHandlers = Arrays.<GetOptions>asList( 
			gr.ntua.ivml.mint.projects.fashion.DatasetOptions::options 
			, gr.ntua.ivml.mint.projects.modemuze.RequestHandler::datasetPublishOptions
			, gr.ntua.ivml.mint.projects.photo.RequestHandler::datasetPublishOptions
			, gr.ntua.ivml.mint.projects.photoxx.RequestHandler::datasetPublishOptions
			, gr.ntua.ivml.mint.projects.efgxx.RequestHandler::datasetPublishOptions
			, gr.ntua.ivml.mint.projects.euscreen.RequestHandler::datasetPublishOptions
			, gr.ntua.ivml.mint.projects.fashionxx.RequestHandler::datasetPublishOptions
			, gr.ntua.ivml.mint.projects.direct.RequestHandler::datasetPublishOptions);
	
	public static int getPathInt( HttpServletRequest request, int pos ) throws Exception  {
		String[] query = request.getPathInfo().split( "/");
		log.debug( "Pathinfo "+ String.join(", ", query));
		
		return Integer.parseInt(query[pos]);
	}
	
	static class TmpFile  implements Closeable {
		File file;
		public TmpFile( File file ) {
			this.file = file;
		}
		
		public void close() {
			file.delete();
		}
	}

	/**
	 * /api/login
	 * @param request
	 * @param response
	 */

	public static void loginPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
//		System.out.println("access to api granted. auth successful");
		User user;
		String username = request.getParameter("username");
		String password = request.getParameter("password");
//		String username=null, password=null;
//		System.out.println(request.getParameterNames().toString());
//		String test = request.getReader().lines().collect(Collectors.joining(System.lineSeparator()));

//		System.out.println(request.getReader().lines().collect(Collectors.joining(System.lineSeparator())));
		ArrayNode res = json.arrayNode();
		ObjectNode responseJson = res.objectNode();

		if( username== null || username.length()==0) {
			errJson("username is required", response);
			return;
		}
		if( password== null || password.length()==0) {
			errJson("password is required", response);
			return;
		}
		user=DB.getUserDAO().getByLoginPassword(username,password);
		if (user!=null) {
			if(!user.isAccountActive()){
				errJson("account is no longer active",response);
				return;
			}
			else if(user.getPasswordExpires()!=null && user.getPasswordExpires().getTime()<(new Date().getTime())){
				errJson("your password has expired",response);
				return;
			}
			else if(Config.getBoolean("disableLogin") && !user.hasRight(User.SUPER_USER)){
				errJson("login is allowed only for superuser",response);
				return;
			}
			else{
				log.debug( "Login successful for user:"+user.getLogin() );
				user.getJobRole();
				request.getSession().setAttribute("user", user);
				responseJson.put("successful", "true");
				res.add(responseJson);
				okJson(res,response);
			}
		}
		else
		{
			errJson("wrong login/password combination",response);
		}

	}

	public static void downloadGet( HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {
		ErrorCondition err = new ErrorCondition();
		try {
			Optional<String> holderIdOpt = RequestHandlerUtil.getUniqueParameter(request, "xpathHolder");
			User u = err.grab(()-> (User) request.getAttribute("user"), "No User logged in");

			if( holderIdOpt.isPresent() ) {
				String holderId = holderIdOpt.get();
				Long id = err.grab( ()->Long.parseLong(holderId), "Invalid id for holder");			
				XpathHolder holder = err.grab( ()->DB.getXpathHolderDAO().getById(id, false), "Unknown Holder");
				err.check( ()->u.can( "view data", holder.getDataset()), "No access rights to data");
				err.onValid( ()->RequestHandlerUtil.downloadValues( holder, response));
				err.onFailed( ()->errJson( err.get(), response ));
				return;
			}
			
			errJson( "Invalid parameters for Download", response );
			
		} catch(Exception e) {
			log.error( "",e );
			errJson( e.getMessage(), response );
		}
	}
	
	
	
	
	/**
	 * /api/enrich
	 * @param request
	 * @param response
	 *
	 * Upload an enrichment CSV
	 * HTTP Header Content-type should be multipart/form-data
	 * Request body should have two parts: file and name
	 */
	public static void enrichPost(HttpServletRequest request, HttpServletResponse response) throws IOException, ServletException {

		ThrowingSupplier<String> exec =  () -> { 
			User u = (User) request.getAttribute("user");
			if (u == null) return "No User logged in";

			//copy request body parts
			Part file = request.getPart("file");
			if( file == null ) return "No 'file' in multipart-request";
			
			Part name = request.getPart("name");
			if( name==null ) return "No 'name' in multipart-request" ;

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
	}

	/**
	 * /api/enrich/:enrichmentId
	 * @param request
	 * @param response
	 */
	public static void enrichGet ( HttpServletRequest request, HttpServletResponse response ) {
		// get enrichment corresponding to id
	}

	/**
	 * /api/enrichExecute?datasetId=xx&enrichmentId=xx
	 * json body .. parameters for columns in Json Array [{"type":"exact"..}{"type":"xmlFragment"..}]
	 * @param request
	 * @param response
	 */
	public static void enrichExecutePost( HttpServletRequest request, HttpServletResponse response ) {

		// throws or returns error message if things go bad
		// otherwise does the job
		ThrowingSupplier<String> exec = () -> {
			User u = (User ) request.getAttribute("user");
			if( u==null ) return  "Login required";
			
			String datasetId = request.getParameter( "datasetId");
			if( datasetId == null ) return "Dataset id parameter missing.";
			
			String enrichmentId = request.getParameter( "enrichmentId");
			if( enrichmentId == null ) return "Enrichment id parameter missing.";
			
			Dataset ds = DB.getDatasetDAO().getById(Long.parseLong(datasetId), false);
			if( ds == null) return "Dataset not found";
			
			Enrichment enrichment = DB.getEnrichmentDAO().getById( Long.parseLong( enrichmentId ), false); 
			if( enrichment == null ) return "Enrichment not found.";

			// check access rights
			boolean dsAccess = false;
			boolean enrichAccess = false;
			
			if( u.isAccessibleOrganization(ds.getOrganization()))
				dsAccess = true;
			
			if( u.isAccessibleOrganization(enrichment.getOrganization()))
					enrichAccess = true;
			
			if( Project.sharedIds(u.getProjectIds(), ds.getProjectIds()).size() > 0 ) dsAccess = true;
			if( Project.sharedIds(u.getProjectIds(), enrichment.getProjectIds()).size() > 0 ) enrichAccess = true;

			if( ! (dsAccess && enrichAccess) ) return "Access not allowed.";

			// this might throw
			ArrayNode config = (ArrayNode) Jackson.om().readTree( request.getReader());

			// lock the dataset
			Lock l =  DB.getLockManager().directLock( u, "offlineTransformation", ds );
			if( l == null ) return "Couldn't aquire lock";
			
			Transformation tr = Transformation.fromDataset( ds, enrichment, config );
			// optionally specify a target schema
			String targetSchemaName = request.getParameter( "targetSchema");
			if( targetSchemaName != null ) {
				XmlSchema targetSchema = DB.getXmlSchemaDAO().getByName(targetSchemaName);
				if( targetSchema != null ) tr.setSchema(targetSchema);
				else log.warn("Target Schema '" + targetSchemaName + "' not found!");
			}
			

			try {
				// test this before we store and queue the thing
				String xsl = tr.getXsl();
				// test if it compiles, it throws if it doesn't
				gr.ntua.ivml.mint.xml.transform.XSLTransform xslTransform = new gr.ntua.ivml.mint.xml.transform.XSLTransform();
				xslTransform.setXSL(xsl);
				xslTransform.getTransformer();
				
				tr.setCreator(u);
				// this might throw ..
				DB.getTransformationDAO().makePersistent(tr);
				// make sure its in the db, readable for the next thread
				DB.commit();
			} catch( Exception e ) {
				// need to release the lock
				DB.getLockManager().releaseLock(l);
				throw  e;
			}
			XSLTransform execTrans = new XSLTransform(tr);
			execTrans.setAquiredLocks( Collections.singletonList(l));
			tr.logEvent("Queued Transformation.");
			Queues.queue( execTrans, "db" );
			ok( response );
			return null;
		};
		
		try { 
			String error = exec.get();
			if( error != null ) errJson( error, response );
		} catch( Exception e ) {
			log.error( "Exception during Enrich execution" , e );
			errJson( e.getMessage(), response );	
		}
	}

	// list the accessible schemas or all if there is no user or superuser
	// returns a json array
	public static void schemaGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			ArrayNode res = json.arrayNode();
			List<XmlSchema> schemas = DB.getXmlSchemaDAO().findAll();
			for (XmlSchema sc : schemas) {
				ObjectNode schemaJson = res.objectNode();
				schemaJson.put("id", sc.getDbID());
				schemaJson.put("name", sc.getName());
				schemaJson.put("file", sc.getXsd());
				res.add(schemaJson);
			}
			okJson(res, response);
		} catch (Exception e) {
			log.error("", e);
			try {
				errJson(e.getMessage(), response);
			} catch (Exception e2) {
				log.error("", e2);
			}
		}
	}

	// Get all projects from the database with the flag "tagged" about the
	// dataset
	public static void projectGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			ArrayNode res = json.arrayNode();
			ObjectMapper mapper = new ObjectMapper();
			User u = (User) request.getAttribute("user");
			if (u == null) {
				errJson("No User logged in", response);
				return;
			}
			String datasetId = request.getParameter("datasetId");
			if (datasetId == null) {
				errJson("No dataset defined", response);
				return;
			}
			List<Project> projects = DB.getProjectDAO().findAll();
			Dataset dataset = DB.getDatasetDAO().getById(Long.parseLong(datasetId), false);
			List<Integer> ids = dataset.getProjectIds();
			projects.stream().forEach(
					p -> res.add(((ObjectNode) mapper.valueToTree(p)).put("tagged", ids.contains((int) p.getDbID()))));
			okJson(res, response);
		} catch (Exception e) {
			log.error("", e);
			try {
				errJson(e.getMessage(), response);
			} catch (Exception e2) {
				log.error("", e2);
			}
		}
	}


	/**
	 * /api/projectAddDataset/:projectId/:datasetId
	 * @param request
	 * @param response
	 */
	public static void projectAddDatasetPost( HttpServletRequest request, HttpServletResponse response ) {
		try {
			ErrorCondition err = new ErrorCondition();

			int datasetId = getPathInt( request, 3 );
			int projectId = getPathInt( request, 2 );
			
			Dataset ds= err.grab( () -> DB.getDatasetDAO().getById( (long) datasetId, false), "Dataset not found." );
			Project p = err.grab( () -> DB.getProjectDAO().getById( (long) projectId, false ), "Project not found." );

			User u = err.grab(()->(User) request.getAttribute("user"), "Login required" );
			
			err.check( () -> ds.getOrganization().getProjectIds().contains( projectId ),
				"Organisation cannot use that project." )
				.check( () -> u.can( "modify data", ds.getOrganization()), "User has no right to do that" );

			err.onValid( ()-> {
				ds.getProjectIds().add( projectId );
				DB.getDatasetDAO().makePersistent(ds);
				okJson( "msg", "Added project " + p.getName() + " to Dataset #" + ds.getDbID(), response );				
			});
			
			err.onFailed(()-> errJson( err.get(), response));
		} catch( Exception e ) {
			errJson( e.getMessage(), response );
		}
	}
	
	/**
	 * /api/projectRemoveDataset/:projectId/:datasetId
	 * @param request
	 * @param response
	 */
	public static void projectRemoveDatasetPost( HttpServletRequest request, HttpServletResponse response ) {
		try {
			ErrorCondition err = new ErrorCondition();

			int datasetId = getPathInt( request, 3 );
			int projectId = getPathInt( request, 2 );
			
			Dataset ds= err.grab( () -> DB.getDatasetDAO().getById( (long) datasetId, false), "Dataset not found." );
			Project p = err.grab( () -> DB.getProjectDAO().getById( (long) projectId, false ), "Project not found." );

			User u = err.grab(()->(User) request.getAttribute("user"), "Login required" );
			
			err.check( () -> u.can( "modify data", ds.getOrganization()), "User has no right to do that" )
				.check( ()-> ds.getProjectIds().contains( projectId ), "Dataset is not in project.");

			err.onValid( ()-> {
				ds.getProjectIds().remove( projectId );
				DB.getDatasetDAO().makePersistent(ds);
				okJson( "msg", "Removed project " + p.getName() + " from Dataset #" + ds.getDbID(), response );				
			});
			
			err.onFailed(()-> errJson( err.get(), response));
		} catch( Exception e ) {
			errJson( e.getMessage(), response );
		}
	}
	
	public static void projectPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			User u = (User) request.getAttribute("user");
			if (u == null) {
				errJson("No User logged in", response);
				return;
			}
			if( !u.hasRight(User.SUPER_USER)) {
				errJson("Need superuser", response);
				return;
			}
			ObjectMapper om = new ObjectMapper();
			Project project = om.readValue(request.getInputStream(), Project.class);
			DB.getProjectDAO().makePersistent(project);
			ok(response);
		} catch (Exception e) {
			log.error("", e);
			try {
				errJson(e.getMessage(), response);
			} catch (Exception e2) {
				log.error("", e2);
			}
		}
	}

	public static void projectLabelPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			ErrorCondition err = new ErrorCondition();

			User u = err.grab(()->(User) request.getAttribute("user"), "No User logged in");
			String datasetId = err.grab(()->request.getParameter("datasetId"), "No dataset specified");
			Dataset dataset = err.grab( () -> DB.getDatasetDAO().getById(Long.parseLong(datasetId), false), "Unknown dataset" );
			err.check( ()-> u.can( "change data", dataset.getOrganization()) ||
					u.sharesProject(dataset), "No modify rights");

			err.onValid( () -> {
				ObjectMapper om = Jackson.om();
				ArrayNode mappingMeta = (ArrayNode) om.readTree(request.getInputStream());
				Map<Integer, Boolean> projectTags = new HashMap<Integer, Boolean>();
				// can not remove project if there is anything happened to the dataset
				boolean canRemoveProject = dataset.getDirectlyDerived().isEmpty();
						
				mappingMeta.forEach(p -> {
					Integer projectId = p.get("dbID").asInt();
					// can not assign project if org doesn't belong to it
					boolean canAddProject = dataset.getOrganization().getProjectIds().contains(projectId);
					
					boolean tag = p.get("tagged").asBoolean();
					if(( tag && canAddProject ) || ((!tag) && canRemoveProject ))
						projectTags.put(projectId, tag);
				});
				dataset.assignToProjects(projectTags);
				ok( response );
			});
			err.onFailed( ()->errJson( err.get(), response ));
		} catch (Exception e) {
			log.error("", e);
			try {
				errJson(e.getMessage(), response);
			} catch (Exception e2) {
				log.error("", e2);
			}
		}
	}
	
	// GET /datasetLabels/#id#
	// label set for projects and folders
	public static void datasetLabelsGet(HttpServletRequest request, HttpServletResponse response) throws Exception {
		try {
			ErrorCondition err = new ErrorCondition();
			ObjectNode res = Jackson.om().createObjectNode();
			int datasetId = getPathInt(request, 2);
			Dataset ds = err.grab(()->DB.getDatasetDAO().getById((long) datasetId, false ), "Illegal Dataset");
			User u = err.grab(()->(User) request.getAttribute("user"), "Need to be logged in");
			boolean projectUser = err.grabBoolean(()->u.sharesProject(ds), "Should not fail" );
			
			err.check(()-> u.can( "view data", ds.getOrganization()) ||
					projectUser, "No rights");
			boolean userCanModify = err.grabBoolean( ()->u.can( "change data", ds.getOrganization()), "Shouldnt fail" );
			boolean dsCanRemoveProjects = err.grabBoolean( ()-> ds.getDirectlyDerived().size()==0, "");
			
			List<Label> allOrgProjects = err.grab(()-> 
				ds.getOrganization()
				.getProjectIds()
				.stream()
				.map( (id) -> new Label( id ))
				.collect( Collectors.toList()), "Shouldnt fail");

			err.onValid(() -> {
				List<Label> labels = allOrgProjects;
				List<String> dsProjects =Arrays.asList( ds.getProjectNames());
				if( labels!= null && labels.size()>0) {
					ArrayNode projectLabels = res.withArray( "projects");
					for( Label l: labels ) {
						ObjectNode on = projectLabels.addObject();
						on.put( "label", l.lblname);
						on.put( "color", l.lblcolor);
						on.put( "selected", dsProjects.contains(l.lblname ));
						on.put( "editable", ( userCanModify || projectUser ) && ( dsCanRemoveProjects || !dsProjects.contains(l.lblname )));
					}
				}
			});
			err.onValid(() -> {
				Collection<String> labels = ds.getFolders()
						.stream()
						.map( l -> new Label(l))
						.map( l -> l.lblname )
						.collect( Collectors.toSet());
				
				Collection<String> orgFolders = ds.getOrganization().getFolders();
				
				if( orgFolders!= null && orgFolders.size()>0) {
					ArrayNode jsonFolders = res.withArray( "folders");
					orgFolders.stream()
					  .map( l -> new Label(l))
					  .forEach( (Label l) -> {
						ObjectNode on = jsonFolders.addObject();
						on.put( "label", l.lblname);
						on.put( "color", l.lblcolor);
						on.put( "editable", userCanModify || projectUser );
						on.put( "selected", labels.contains( l.lblname ));
					} );
				}
			});
			err.onValid( () -> okJson( res, response  ));
			err.onFailed(()->errJson( err.get(), response ));
			
		} catch ( Exception e ) {
			log.error( "", e);
			errJson( e.getMessage(), response );
		}
	}
	
	public static void datasetLabelsPost(HttpServletRequest request, HttpServletResponse response) throws Exception {
		try {
			ErrorCondition err = new ErrorCondition();
			int datasetId = getPathInt(request, 2);
			Dataset ds = err.grab(()->DB.getDatasetDAO().getById((long) datasetId, false ), "Illegal Dataset");
			User u = err.grab(()->(User) request.getAttribute("user"), "Need to be logged in");
			boolean projectUser = err.grabBoolean(()->u.sharesProject(ds), "Should not fail" );
			
			err.check(()-> u.can( "change data", ds.getOrganization()) ||
					projectUser, "No rights");
			boolean userCanModify = err.grabBoolean( ()->u.can( "change data", ds.getOrganization()), "Shouldnt fail" );
			boolean dsCanRemoveProjects = err.grabBoolean( ()-> ds.getDirectlyDerived().size()==0, "");
			ObjectNode content = err.grab( ()-> (ObjectNode) Jackson.om.readTree(request.getInputStream()), "Illegal Content" );
			
			Map<String,Project> allOrgProjects = err.grab(()-> 
				ds.getOrganization()
				.getProjectIds()
				.stream()
				.map( (id) -> DB.getProjectDAO().getById( (long) id, false ))
				.collect( Collectors.toMap(
						(p)->p.getName(), 
						(p)->p 
						)), "Shouldnt fail");

			// map folder name -> folder name+color
			Map<String, String> allOrgFolderNames = err.grab( () ->
				ds.getOrganization().getFolders().stream()
					.collect( Collectors.toMap( 
							(l) -> (new Label(l)).lblname,
							(l) -> l
							)), "No problems here" );
			
			// we need to set the project ids in ds from incoming json
			err.onValid(() -> {
				List<Integer> newProjectIds = new ArrayList<>();
				List<Integer> oldProjectIds = ds.getProjectIds();
				
				content.withArray( "projects" ).forEach(
					( node) -> {
						String projectName = ((ObjectNode) node).get( "label").textValue();
						boolean selected = ((ObjectNode) node).get( "selected").booleanValue();
						if( selected ) {
							Project p  = allOrgProjects.get( projectName );
							if( p != null ) newProjectIds.add( (int) p.getDbID());
						}
				}); 
				
				if( !dsCanRemoveProjects ) {
					// add all project ids from before
					for( Integer oldProjectId: oldProjectIds) {
						if( !newProjectIds.contains(oldProjectId)) newProjectIds.add( oldProjectId );
					}
				}
				// only if we can actually modify the projects
				if( userCanModify || (
					projectUser && !Project.sharedIds(newProjectIds, u.getProjectIds()).isEmpty()))
					ds.setProjectIds( newProjectIds ); 
			} );

			// and update the folders, should be simple 
			err.onValid( () -> {
				ArrayNode jsonFoldersArrayNode = Jackson.om().createArrayNode();
				content.withArray( "folders" ).forEach(( node) -> {
					String folderName = ((ObjectNode) node).get( "label").textValue();
					String folder = allOrgFolderNames.get( folderName );
					boolean selected = ((ObjectNode) node).get( "selected").booleanValue();
					if((folder != null) && selected ) jsonFoldersArrayNode.add( folder );
				} );
				ds.setJsonFolders(jsonFoldersArrayNode.toString());
			});
			
			err.onValid( () -> {
				DB.getDatasetDAO().makePersistent(ds);
				ok( response );
			});
			err.onFailed(()->errJson( err.get(), response ));
			
		} catch ( Exception e ) {
			log.error( "", e);
			errJson( e.getMessage(), response );
		}
	}

	// Get all projects labels about the dataset
	public static void projectLabelsGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			ObjectMapper mapper = new ObjectMapper();
			User u = (User) request.getAttribute("user");
			if (u == null) {
				errJson("No User logged in", response);
				return;
			}
			String datasetId = request.getParameter("datasetId");
			if (datasetId == null) {
				errJson("No dataset defined", response);
				return;
			}
			Dataset dataset = DB.getDatasetDAO().getById(Long.parseLong(datasetId), false);
			List<Label> projectLabels = dataset.getProjectLabels();
			ArrayNode res = mapper.valueToTree(projectLabels);
			okJson(res, response);
		} catch (Exception e) {
			log.error("", e);
			try {
				errJson(e.getMessage(), response);
			} catch (Exception e2) {
				log.error("", e2);
			}
		}
	}

	// Get all dataset belonging to the project
	public static void projectDatasetsGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			ErrorCondition err = new ErrorCondition();
			User u = err.grab(()->(User) request.getAttribute("user"), "Login required" );


			ObjectMapper mapper = new ObjectMapper();
			String projectId = err.grab(()->request.getParameter("projectId"),"No project defined");
			err.grab( () -> DB.getProjectDAO().getById( Long.parseLong( projectId ), false ), "Project not found." );

			err.onValid( () -> {
				List<Dataset> datasets = DB.getDatasetDAO().findNonDerivedByProjectFolders(null, Long.parseLong(projectId));
				// filter by the ones the user can see 
				JsonNode res = mapper.valueToTree(datasets);
				okJson(res, response);
			});
			err.onFailed(()-> errJson( err.get(), response));

		} catch (Exception e) {
			log.error("", e);
			errJson(e.getMessage(), response);
		}
	}

	// logged in user accessible organizations
	public static void accessibleOrganizationGet(HttpServletRequest request, HttpServletResponse response)
			throws IOException {
		try {
			ArrayNode res = json.arrayNode();
			User u = (User) request.getAttribute("user");
			if (u == null) {
				errJson("No User logged in", response);
				return;
			}
			List<Organization> orgs = u.getAccessibleOrganizations();

			for (Organization org : orgs) {
				ObjectNode orgJson = res.objectNode();
				orgJson.put("id", org.getDbID());
				orgJson.put("shortName", org.getShortName());
				orgJson.put("englishName", org.getEnglishName());
				orgJson.put("originalName", org.getOriginalName());
				res.add(orgJson);
			}
			okJson(res, response);
		} catch (Exception e) {
			log.error("", e);
			try {
				errJson(e.getMessage(), response);
			} catch (Exception e2) {
				log.error("", e2);
			}
		}
	}

	public static void tokenGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		User user = (User) request.getAttribute("user");
		Long userId;
		if (user == null)
			response.setStatus(HttpServletResponse.SC_FORBIDDEN);
		else {
			userId = user.getDbID();
			try {
				String token = TokenManager.encrypt(userId);
				response.setContentType("text/plain");
				PrintWriter out = response.getWriter();
				out.println(token);
				out.close();
			} catch (Exception e) {
				log.error(e.getMessage(), e);
			}

		}
	}

	// accept an extended mapping json posted ...
	public static void mappingPost(HttpServletRequest request, HttpServletResponse response) {
		try {
			User user = (User) request.getAttribute("user");
			if (user == null) {
				errJson("No authenticated User", response);
				return;
			}

			ObjectMapper om = new ObjectMapper();
			ObjectNode mappingMeta = (ObjectNode) om.readTree(request.getInputStream());
			MappingImportHandler mih = new MappingImportHandler(mappingMeta, user.getDbID());

			Queues.queue(mih, "db");

			// empty json success response
			okJson(om.createObjectNode(), response);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			try {
				errJson("Exception " + e.getMessage(), response);
			} catch (Exception e2) {
				log.error("", e2);
			}
		}

	}

	public static void datasetPost(HttpServletRequest request, HttpServletResponse response) {
		try {
			User user = (User) request.getAttribute("user");
			if (user == null) {
				errJson("No authenticated User", response);
				return;
			}

			UUID handlerId = UUID.randomUUID();
			ObjectMapper om = new ObjectMapper();
			ObjectNode dsMeta = (ObjectNode) om.readTree(request.getInputStream());
			DatasetImportHandler dih = new DatasetImportHandler(dsMeta, user.getDbID());

			FileUploader.installStreamHandler(handlerId, dih);

			okJson("uploadUrl", "api/upload/" + handlerId, response);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			try {
				errJson("Exception " + e.getMessage(), response);
			} catch (Exception e2) {
				log.error("", e2);
			}
		}
	}

	/**
	 * Adds all the declared DatasetOptions providing methods results to the result.
	 * Complains if there is a problem. Does not check the rights of the user to read the dataset,
	 * each of the DatasetOptions providers should do that themselves.
	 * @param request
	 * @param response
	 * @throws Exception
	 */
	public static void datasetOptionsGet(HttpServletRequest request, HttpServletResponse response) {
		try {
			ErrorCondition err = new ErrorCondition();
			User u = err.grab(()->(User) request.getAttribute("user"), "Login required" );

			int datasetId = getPathInt( request, 2 );
			Dataset ds = err.grab(()->DB.getDatasetDAO().getById((long) datasetId, false ), "Dataset not found");

			boolean projectUser = err.grabBoolean(()->u.sharesProject(ds.getOrigin()), "Should not fail" );
			

			err.check(()-> u.can( "view data", ds.getOrganization()) || projectUser, "No rights");
			
			err.onValid( () -> {
				ArrayNode res = Jackson.om().createArrayNode();
				// check all the options providers
				for( GetOptions go: optionsHandlers ) {
					res.addAll( go.options(ds, u ));
				}
				okJson(res,response);
			});

			err.onFailed(()-> errJson( err.get(), response));

		} catch (Exception e) {
			log.error(e.getMessage(), e);
			errJson( e.getMessage(), response);
		}
	}
	
	
	public static void uploadPost(HttpServletRequest request, HttpServletResponse response) {
		response.setContentType("application/json");
		try {
			String handlerId = request.getPathInfo().split("/")[2];
			FileUploader.executeStreamHandler(UUID.fromString(handlerId), request.getInputStream());
			ok(response);
		} catch (Exception e) {
			log.error(e.getMessage(), e);
			err(response);
		}

	}

	public static void searchGet(HttpServletRequest request, HttpServletResponse response) throws Exception {
		User user = (User) request.getAttribute("user");
		Map<String, String[]> parameters = request.getParameterMap();
		JsonNode res = Search.search(user, parameters);
		okJson(res, response);
	}

	public static void errJson(String msg, HttpServletResponse response) {
		try {
			response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
			response.setContentType("application/json");
			PrintWriter out = response.getWriter();
			ObjectNode json = Jackson.om().createObjectNode();
			json.put("error", msg);
			out.println(json);
			out.close();
		} catch( Exception e ) {
			log.error( "Cannot generate error response" ,e );
		}
	}

	public static void okJson(String key, String val, HttpServletResponse response) {
		try {
			ObjectNode res = Jackson.om().createObjectNode();
			res.put(key, val);
			okJson(res, response);
		} catch( Exception e ) {
			log.error( "Cannot create response" ,e );
		}
	}

	public static void okJson(JsonNode json, HttpServletResponse response) {
		try {
			response.setStatus(HttpServletResponse.SC_OK);
			response.setContentType("application/json");
			PrintWriter out = response.getWriter();
			out.println(json);
			out.close();
		} catch( Exception e ) {
			log.error( "Cannot create response" ,e );
		}			
	}

	public static void ok(HttpServletResponse response) {
		try {
			response.setStatus(HttpServletResponse.SC_OK);
			response.getOutputStream().close();
		} catch( Exception e ) {
			log.error( "Cannot create response" ,e );
		}			
	}

	public static void err(HttpServletResponse response) {
		try {
			response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
			response.getOutputStream().close();
		} catch( Exception e ) {
			log.error( "Cannot create response" ,e );
		}			
	}
}
