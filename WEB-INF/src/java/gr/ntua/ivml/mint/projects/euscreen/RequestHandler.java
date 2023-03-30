package gr.ntua.ivml.mint.projects.euscreen;

import static gr.ntua.ivml.mint.api.RequestHandler.errJson;
import static gr.ntua.ivml.mint.api.RequestHandler.okJson;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.node.ArrayNode;

import gr.ntua.ivml.mint.api.RequestHandlerUtil;
import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.f.ErrorCondition;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.PublicationRecord;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.util.Config;
import gr.ntua.ivml.mint.util.Jackson;
import gr.ntua.ivml.mint.util.PublicationHelper;

/**
 * Static messages routed here by the router servlet, starting with
 * /api/euscreen/...
 * 
 * OAI publications go to two different oai places (should be euscreenxl and euscreenxl-core on panic:9000
 * if your datasets was published on portal, it belongs to the ..-core otherwise its the normal one.
 * 
 * @author stabenau
 *
 */
public class RequestHandler {

	public static final Logger log = Logger.getLogger(RequestHandler.class);

	public static class Option {
		public String project = "euscreen";
		public String category = "publication";
		public String label;
		public boolean inProgress = false;
		public String url;

		// json|htmlNewPanel|htmlReplacePanel
		public String response = "json";
	};

	
	public static void europeanaUnpublishGet(HttpServletRequest request, HttpServletResponse response )
			throws IOException {
		try {
			ErrorCondition err = new ErrorCondition();
			Dataset ds = RequestHandlerUtil.accessDataset(request, response, "euscreen", err);

			Optional<String> errMsg = err.grab(()->EuscreenPublish.europeanaUnpublishApi(ds),"Api call failed" );
			err.check( ()->!errMsg.isPresent(), errMsg.orElse(""));

			err.onValid( ()-> okJson("msg", "OAI unpublish succeeded.", response));
			err.onFailed(() -> errJson(err.get(), response));
		} catch (Exception e) {
			log.error("", e);
			errJson("Something went wrong: '" + e.getMessage() + "'", response);
			return;
		}
	}

	// maybe the same as unpublish
	public static void europeanaPublishCleanGet(HttpServletRequest request, HttpServletResponse response)
			throws IOException {
		europeanaUnpublishGet(request, response);
	}

	public static void portalPublishGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		EuscreenPublish.portalPublishGet(request, response);
	}

	public static void portalUnpublishGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		EuscreenPublish.portalUnpublishGet(request, response);
	}

	public static void portalPublishCleanGet(HttpServletRequest request, HttpServletResponse response)
			throws IOException {
		EuscreenPublish.portalUnpublishGet(request, response);
	}

	public static void corePrepareEdmGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		EuscreenPublish.corePrepareEdmGet(request, response);
	}	

	public static void europeanaPublishGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
		try {
			ErrorCondition err = new ErrorCondition();
			Dataset ds = RequestHandlerUtil.accessDataset(request, response, "euscreen", err);
			final User publisher = err.grab(() -> (User) request.getAttribute("user"), "No User logged in");

			// move all non external check into usable function
			Optional<String> errMsg = err.grab(()-> EuscreenPublish.europeanaPublishApi(ds, publisher),"Api call failed" );
			err.check( ()->!errMsg.isPresent(), errMsg.orElse(""));
			
			err.onValid(() -> okJson("msg", "Publication to OAI queued.", response));
			err.onFailed(() -> errJson(err.get(), response));

		} catch (Exception e) {
			log.error("", e);
			errJson("Something went wrong: '" + e.getMessage() + "'", response);
			return;
		}
	}

	// adds portal options to datasetOptions, needs ds and list of publication
	// records for this ds as arguments
	public static void addPortalOptions( Dataset ds,List<PublicationRecord> pubs, ArrayNode datasetOptions ) {

		Optional<PublicationRecord>  prOpt = pubs.stream().filter( pr -> pr.getTarget().equals( "euscreen.portal")).findAny();
		if( prOpt.isPresent()) {
			Option option = new Option();
			switch( prOpt.get().getStatus() ) {
			case PublicationRecord.PUBLICATION_RUNNING:
				option.label = "Portal publication running";
				option.inProgress = true;
				option.url = null;
				break;
			case PublicationRecord.PUBLICATION_OK:
				// if there is no dataset depending on this, offer the option to prepare for europeana publish
				if(( ds.getDirectlyDerived().size() == 0 ) && (prOpt.get().getPublishedDataset().getDbID() == ds.getDbID())) {
					Option prepare = new Option();
					prepare.label = "Prepare for Europeana publish";
					prepare.url = "api/euscreen/corePrepareEdm/"+prOpt.get().getPublishedDataset().getDbID();
					datasetOptions.add( Jackson.om().valueToTree(prepare));					
				}
				option.label = "Unpublish from Portal";
				option.url = "api/euscreen/portalUnpublish/"+prOpt.get().getPublishedDataset().getDbID();
				break;
			case PublicationRecord.PUBLICATION_FAILED:
				option.label = "Clean failed portal Publication";
				option.url = "api/euscreen/portalPublishClean/"+prOpt.get().getPublishedDataset().getDbID();
				break;
			}
			datasetOptions.add( Jackson.om().valueToTree(option));
		} else {
			// can we publish on portal
			Optional<Dataset> oDs = PublicationHelper.uniqueSuitableDataset(ds, Config.get( "euscreen.portal.schema"));
			if( oDs.isPresent()) {
				Option option = new Option();
				option.label = "Publish to Portal";
				option.url = "api/euscreen/portalPublish/"+oDs.get().getDbID();
				datasetOptions.add( Jackson.om().valueToTree(option));
			}		
		}
	}

	// adds portal options to datasetOptions, needs ds and list of publication
	// records for this ds as arguments
	public static void addEuropeanaOptions( Dataset ds,List<PublicationRecord> pubs, ArrayNode datasetOptions ) {

		Optional<PublicationRecord>  prOpt = pubs.stream().filter( pr -> pr.getTarget().equals( "euscreen.oai")).findAny();
		if( prOpt.isPresent()) {
			Option option = new Option();
			switch( prOpt.get().getStatus() ) {
			case PublicationRecord.PUBLICATION_RUNNING:
				option.label = "Europeana publication running";
				option.inProgress = true;
				option.url = null;
				break;
			case PublicationRecord.PUBLICATION_OK:
				// if there is no dataset depending on this, offer the option to prepare for europeana publish
				option.label = "Unpublish from Europeana";
				option.url = "api/euscreen/europeanaUnpublish/"+prOpt.get().getPublishedDataset().getDbID();
				break;
			case PublicationRecord.PUBLICATION_FAILED:
				option.label = "Clean failed Europeana Publication";
				option.url = "api/euscreen/europeanaPublishClean/"+prOpt.get().getPublishedDataset().getDbID();
				break;
			}
			datasetOptions.add( Jackson.om().valueToTree(option));
		} 
		
		else {
			Optional<Dataset> oDs = PublicationHelper.uniqueSuitableDataset(ds, Config.get( "euscreen.oai.schema"));
			if( oDs.isPresent()) {
				Option option = new Option();
				option.label = "Publish to Europeana";
				option.url = "api/euscreen/europeanaPublish/"+oDs.get().getDbID();
				datasetOptions.add( Jackson.om().valueToTree(option));
			}					
		}
	}
	
	
	
	public static ArrayNode datasetPublishOptions( Dataset ds, User user ) {
		ArrayNode result = Jackson.om().createArrayNode();
		
		List<String> projects = Arrays.asList( ds.getOrigin().getProjectNames());
		// maybe nothing to do here, although we only get here if there is?
		if( !projects.contains("euscreen")) return result;

		// 1: publish to Europeana , unpublish, clean, running
		// 2: pubish to portal, unpublish portal
		// 3: prepare for europeana (on portal published sets, will compute an EDM set)
		// 4: publish XX for EDM enriched, not published to europeana
		// target euscreen, euscreenxx, portal
		
		List<PublicationRecord> pubs = DB.getPublicationRecordDAO().findByOriginalDataset( ds.getOrigin());
		addPortalOptions( ds, pubs, result );
		addEuropeanaOptions(ds, pubs, result);
		return result;
	}
}
