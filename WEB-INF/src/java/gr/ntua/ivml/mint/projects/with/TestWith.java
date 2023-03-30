package gr.ntua.ivml.mint.projects.with;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.Dataset;
import gr.ntua.ivml.mint.persistent.Item;
import gr.ntua.ivml.mint.persistent.PublicationRecord;
import gr.ntua.ivml.mint.projects.with.model.With;
import gr.ntua.ivml.mint.util.ApplyI;
import gr.ntua.ivml.mint.util.Config;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import net.sf.json.JSONArray;

public class TestWith implements Runnable {
	public static final Logger log = Logger.getLogger(TestWith.class);
	Dataset dataset;
	PublicationRecord pr;
	HttpClient hc;
	String collectionId;
	String schemaName;
	String collectionName;
	Boolean isPublic;
	String collectionDescription;
	// private HttpClient hc;

	public TestWith(Dataset ds, PublicationRecord pr, String collectionId, HttpClient hc,
			String schemaName, String collectionName, Boolean isPublic, String collectionDescription) throws Exception {
		this.collectionId = collectionId;
		this.dataset = ds;
		this.pr = pr;
		this.hc = hc;
		this.schemaName = schemaName;
		this.collectionName = collectionName;
		this.isPublic = isPublic;
		this.collectionDescription = collectionDescription;
	}

	@Override
	public void run() {
		log.debug("with export started");
		try {
			DB.getSession().beginTransaction();
			DB.getStatelessSession().beginTransaction();
			dataset = DB.getDatasetDAO().getById(dataset.getDbID(), false);
			runInThread();
		} catch (Exception e) {
			log.info("Error while exporting ", e);
		} finally {
			DB.commit();
			DB.closeSession();
			DB.closeStatelessSession();
		}

	}
	
	
	public String makeCollection(String collectionName,Boolean isPublic,String collectionDescription) throws ClientProtocolException, IOException {
		HttpPost createCollection = new HttpPost(Config.get("with.backend") + "/collection");
		ObjectMapper om = new ObjectMapper();
		ObjectNode jsonBody = om.createObjectNode();
		jsonBody.with( "administrative").with( "access").put( "isPublic", isPublic );

		ObjectNode descriptiveData = jsonBody.with("descriptiveData");
		descriptiveData.with( "label")
			.withArray( "def").add( collectionName );
		descriptiveData.with( "label")
			.withArray( "en").add( collectionName );
		descriptiveData.with( "description")
			.withArray( "def").add( collectionDescription );
		descriptiveData.with( "description")
			.withArray( "en").add( collectionDescription );
		
		createCollection.setEntity(new StringEntity(jsonBody.toString()));
		createCollection.addHeader("Content-type", "text/json");

		HttpResponse response = hc.execute(createCollection);

		String jsonResponse = EntityUtils.toString(response.getEntity(), "UTF8");
		createCollection.releaseConnection();
		createCollection.abort();

		final String collectionId = om.readTree(jsonResponse).at("dbId").asText();
		return collectionId;
	}

	private void runInThread() throws Exception {

		try {
			final Dataset publishDataset = dataset.getBySchemaName(schemaName);
			final ObjectMapper om = new ObjectMapper();
			
			ApplyI<Item> itemSender = new ApplyI<Item>() {

				Integer counter = 0;

				public void apply(Item item) throws Exception {

					ObjectNode result;
					try {
						With withObject = new With(item.getDocument());

						result = om.valueToTree(withObject);
						String jsonBody = result.toString();
					//	log.debug("Item is: " + jsonBody);

						HttpPost create = new HttpPost(
								Config.get("with.backend") + "/collection/" + collectionId + "/addRecord");
						create.setEntity(new StringEntity(jsonBody, "UTF-8"));
						create.addHeader("Content-type", "text/json");
						HttpResponse response = hc.execute(create);

						String jsonResponse = EntityUtils.toString(response.getEntity(), "UTF8");

					//	log.debug("Response is: " + jsonResponse);
						create.releaseConnection();
						create.abort();
						// log.debug("Item added response: " + jsonResponse);
						if (response.getStatusLine().getStatusCode() != 200) {
							dataset.logEvent("Item failed export to With" + " " + item.getDbID().toString());
							// dataset.logEvent(jsonBody);
							dataset.logEvent("Item added response: " + jsonResponse);
						}

					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
						dataset.logEvent("Item failed export to With");
						dataset.logEvent(item.getLabel());
						dataset.logEvent(item.getDbID().toString());
						dataset.logEvent("Item failed export to With" + " " + item.getDbID().toString(),
								item.getDataset().getCreator());
					}

					counter++;
					if (counter % 5000 == 0) {
						Integer number = (counter / 5000) + 1;
						String id = makeCollection(collectionName + " " + number , isPublic, collectionDescription);
						collectionId = id;
					}
				}

			};

			publishDataset.processAllValidItems(itemSender, true);

			pr.setStatus(PublicationRecord.PUBLICATION_OK);
			pr.setPublishedItemCount(dataset.getValidItemCount());
			dataset.logEvent("Finished export to With", "");
			DB.commit();
		} catch (Exception e) {
			pr.setStatus(PublicationRecord.PUBLICATION_FAILED);
			log.error("There was a problem during export of the  dataset to With !", e);
			dataset.logEvent("There was a problem while during export of the dataset to With!");
			DB.commit();
		} finally {
			DB.getPublicationRecordDAO().makeTransient(pr);
		}
	}

}
