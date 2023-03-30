package gr.ntua.ivml.mint.persistent;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class AnnotationSet {

	private static final Logger log = LogManager.getLogger();

	private Long dbID;
	private String name;
	private Date  creationDate;
    private List<Integer> projectIds = new ArrayList<>();

	private Organization organization;
	private Dataset targetDataset;
	private Dataset targetOriginalDataset;
	
	private byte[] payload;

	// will  contain at least the format of the payload, other configs can be done in here too
	private String configJson;

	// all supported annotation set formats
	public static enum Format {
		GZIP_JSON_LD,
		GZIP_CSV
	}
	
	/*
	 * Util 
	 */
	
	private static ObjectMapper om = new ObjectMapper();
	static {
		om.enable(SerializationFeature.INDENT_OUTPUT);
	}

	public JsonNode payloadAsJson() {
		try {
			return om.readTree(payloadAsUtf8String());
		} catch( Exception e ) {
			log.error( "Invalid Json", e );
			return null;
		}
	}
	
	public String payloadAsUtf8String() {
		try ( ByteArrayInputStream bis = new ByteArrayInputStream(payload);
			   GZIPInputStream gzip = new GZIPInputStream(bis);
				InputStreamReader isr = new InputStreamReader(gzip, "UTF8");
				)
				{
				return IOUtils.toString( isr );
		} catch( Exception e ) {
			log.error( "unzip failed for payload", e );
			return "";
		}
	}
	
	public void setPayloadFromString( String input ) {
		try (
			ByteArrayOutputStream bos = new ByteArrayOutputStream( );
			GZIPOutputStream goz = new GZIPOutputStream( bos );
				OutputStreamWriter ow = new OutputStreamWriter(goz, "UTF8" )
				) {
			ow.write(input);
			ow.flush();
			payload = bos.toByteArray();
		} catch( Exception e ) {
			log.error( "Zipping payload failed", e );
		}
	}
	
	public void jsonToPayload( JsonNode json ) {
		try {
			String jsonString = om.writeValueAsString(json);
			setPayloadFromString(jsonString);
		} catch( Exception e ) {
			log.error( "Json serialize failed, payload unchanged", e );
		}
	}
	
	public void setConfigAsJson( JsonNode config ) {
		try {
			configJson = om.writeValueAsString(config);
		} catch( Exception e ) {
			log.error( "Could not serialize json", e );
		}
	}
	
	
	// returns null on invalid config json string
	public JsonNode getConfigAsJson() {
		try {
			return om.readTree(configJson);
		} catch( Exception e ) {
			log.error( "Invalid Json", e );
			return null;
		}
	}
 	

	
	/*
	 * 
	 * BS Getter setter section
	 * 
	 */
	public Long getDbID() {
		return dbID;
	}

	public void setDbID(Long dbID) {
		this.dbID = dbID;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Date getCreationDate() {
		return creationDate;
	}

	public void setCreationDate(Date creationDate) {
		this.creationDate = creationDate;
	}

	public Organization getOrganization() {
		return organization;
	}

	public void setOrganization(Organization organization) {
		this.organization = organization;
	}

	public Dataset getTargetDataset() {
		return targetDataset;
	}

	public void setTargetDataset(Dataset targetDataset) {
		this.targetDataset = targetDataset;
	}

	public Dataset getTargetOriginalDataset() {
		return targetOriginalDataset;
	}

	public void setTargetOriginalDataset(Dataset targetOriginalDataset) {
		this.targetOriginalDataset = targetOriginalDataset;
	}

	public byte[] getPayload() {
		return payload;
	}

	public void setPayload(byte[] payload) {
		this.payload = payload;
	}

	public String getConfigJson() {
		return configJson;
	}

	public void setConfigJson(String configJson) {
		this.configJson = configJson;
	}
}
