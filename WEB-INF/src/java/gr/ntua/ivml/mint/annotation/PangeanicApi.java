package gr.ntua.ivml.mint.annotation;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.http.client.fluent.Request;
import org.apache.http.entity.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import gr.ntua.ivml.mint.util.Config;

public class PangeanicApi {
	  // Definitions
	
	private static final Logger log = LogManager.getLogger();
	
    private static String translationUrl = "http://prod.pangeamt.com:8080/NexRelay/v1/translate";
    private static String languageDetectionUrl = "http://prod.pangeamt.com:8080/NexRelay/v1/detect_language";
    private static String apiKey = null; // set it in local.conf

    // POJOs for parsing endpoint responses
    private static class LanguageDetectSingleResponse {
        private String src_detected;
        private double src_lang_score;

        public String getSrc_detected() {
            return src_detected;
        }

        public void setSrc_detected(String src_detected) {
            this.src_detected = src_detected;
        }

        public double getSrc_lang_score() {
            return src_lang_score;
        }

        public void setSrc_lang_score(double src_lang_score) {
            this.src_lang_score = src_lang_score;
        }

        @Override
        public String toString() {
            return "LanguageDetectSingleResponse{" +
                    "src_lang_detected='" + src_detected + '\'' +
                    ", src_lang_score='" + src_lang_score + '\'' +
                    '}';
        }
    }

    private static class LanguageDetectionResponse {
        private List<LanguageDetectSingleResponse> detected_langs;

        public List<LanguageDetectSingleResponse> getDetected_langs() {
            return detected_langs;
        }

        public void setDetected_langs(List<LanguageDetectSingleResponse> detected_langs) {
            this.detected_langs = detected_langs;
        }

        @Override
        public String toString() {
            return "LanguageDetectionResponse{" +
                    "detected_langs=" + detected_langs +
                    '}';
        }
    }

    private static class TranslationSingleResponse {
        private String src;
        private String tgt;
        private double score;

        public String getSrc() {
            return src;
        }

        public void setSrc(String src) {
            this.src = src;
        }

        public String getTgt() {
            return tgt;
        }

        public void setTgt(String tgt) {
            this.tgt = tgt;
        }

        public double getScore() {
            return score;
        }

        public void setScore(double score) {
            this.score = score;
        }
    }

    private static class TranslationResponse {
        private List<TranslationSingleResponse> translations;
        private String src_lang;
        private String tgt_lang;

        public TranslationSingleResponse getTranslationBySource (String source) {
            return translations.stream().filter(trans -> source.equals(trans.getSrc())).findFirst().orElse(null);
        }
        public List<TranslationSingleResponse> getTranslations() {
            return translations;
        }

        public void setTranslations(List<TranslationSingleResponse> translations) {
            this.translations = translations;
        }

        public String getSrc_lang() {
            return src_lang;
        }

        public void setSrc_lang(String src_lang) {
            this.src_lang = src_lang;
        }

        public String getTgt_lang() {
            return tgt_lang;
        }

        public void setTgt_lang(String tgt_lang) {
            this.tgt_lang = tgt_lang;
        }
    }

    // Main object for translations
    public static class TranslationDetails {
        private String originalValue;
        private String detectedLanguage;
        private double detectedLanguageScore;
        private String translatedValue;
        private double translationScore;

        public TranslationDetails(String originalValue) {
            this.originalValue = originalValue;
        }

        public double getTranslationScore() {
            return translationScore;
        }

        public void setTranslationScore(double translationScore) {
            this.translationScore = translationScore;
        }

        public double getDetectedLanguageScore() {
            return detectedLanguageScore;
        }

        public void setDetectedLanguageScore(double detectedLanguageScore) {
            this.detectedLanguageScore = detectedLanguageScore;
        }

        public String getOriginalValue() {
            return originalValue;
        }

        public void setOriginalValue(String originalValue) {
            this.originalValue = originalValue;
        }

        public String getDetectedLanguage() {
            return detectedLanguage;
        }

        public void setDetectedLanguage(String detectedLanguage) {
            this.detectedLanguage = detectedLanguage;
        }

        public String getTranslatedValue() {
            return translatedValue;
        }

        public void setTranslatedValue(String translatedValue) {
            this.translatedValue = translatedValue;
        }

        @Override
        public String toString() {
            return "TranslationDetails{" +
                    "originalValue='" + originalValue + '\'' +
                    ", detectedLanguage='" + detectedLanguage + '\'' +
                    ", detectedLanguageScore=" + detectedLanguageScore +
                    ", translatedValue='" + translatedValue + '\'' +
                    ", translationScore=" + translationScore +
                    '}';
        }
    }

    public static JsonNode createAnnotationBody(String translation) {
        ObjectMapper om = new ObjectMapper();
        JsonNode body = om.createObjectNode();
        ((ObjectNode) body).put("type", "TextualBody");
        ((ObjectNode) body).put("value", translation);
        ((ObjectNode) body).put("language", "en");

        return body;
    }

    // should all have the same src language.
    // result is filled into the trasnlation details
    public static void translate(List<TranslationDetails> value) throws IOException {

        ObjectMapper om = new ObjectMapper();
        JsonNode requestBody = om.createObjectNode();

        ((ObjectNode) requestBody).put("apikey", getApiKey());
        ((ObjectNode) requestBody).put("mode", "EUROPEANA");
        ((ObjectNode) requestBody).putArray("src");

        for( TranslationDetails detail: value ) {
            ((ArrayNode) ((ObjectNode) requestBody).get("src")).add(detail.getOriginalValue());        	
        }

        ((ObjectNode) requestBody).put("src_lang", value.get(0).getDetectedLanguage());
        ((ObjectNode) requestBody).put("include_src", true);
        ((ObjectNode) requestBody).put("tgt_lang", "en");
        String response = null;
        try {
	        response = Request.Post(translationUrl).bodyString(requestBody.toString(), ContentType.APPLICATION_JSON)
	                .socketTimeout(5000)
	                .execute()
	                .returnContent()
	                .toString();
	        
	        TranslationResponse res = om.readValue(response, TranslationResponse.class);
	        Iterator<TranslationSingleResponse> itTrans = res.translations.iterator();
	        Iterator<TranslationDetails> itRes = value.iterator();
	        while( itTrans.hasNext() && itRes.hasNext() ) {
	        	TranslationSingleResponse tsr = itTrans.next();
	        	TranslationDetails detail = itRes.next();
	        	detail.translatedValue = tsr.getTgt();
	        	detail.translationScore = tsr.getScore();
	        	
	        	if( ! tsr.getSrc().equals( detail.getOriginalValue())) 
	        			log.warn( "Inconsistent Translation: \n" + 
	        					" SrcOriginal: " + detail.getOriginalValue() + "\n" +
	        					" ApiReturn  : " + tsr.getSrc() + "\n" );
	        }
        } catch( Exception e ) {
        	log.error( "Request: \n" + requestBody.toString()+"\n\n");
        	log.error( "Response: \n" + response + "\n\n");
        	log.error( "Exception during translation", e );
        	throw e;
        }

    }

    public static List<TranslationDetails> languageDetect(List<TranslationDetails> values) throws IOException {

        ObjectMapper om = new ObjectMapper();
        JsonNode requestBody = om.createObjectNode();
        ((ObjectNode) requestBody).put("apikey", getApiKey());
        ((ObjectNode) requestBody).put("mode", "EUROPEANA");
        ((ObjectNode) requestBody).putArray("src");

        for (TranslationDetails value : values) {
            ((ArrayNode) ((ObjectNode) requestBody).get("src")).add(value.getOriginalValue());
        }

        String response = Request.Post(languageDetectionUrl).bodyString(requestBody.toString(), ContentType.APPLICATION_JSON)
                            .socketTimeout(5000)
                            .execute()
                            .returnContent()
                            .toString();

        LanguageDetectionResponse res = om.readValue(response, LanguageDetectionResponse.class);
        List<LanguageDetectSingleResponse> responseList = res.getDetected_langs();
        for (int i=0; i < responseList.size(); i++) {
            values.get(i).setDetectedLanguage(responseList.get(i).getSrc_detected());
            values.get(i).setDetectedLanguageScore(responseList.get(i).getSrc_lang_score());
        }

        return values;
    }
    
    private static String getApiKey() {
    	if( apiKey == null ) {
    		apiKey = Config.get( "pageanic.apikey");
    	}
    	return apiKey;
    }

    public static void main(String[] args) throws IOException {
        List<TranslationDetails> myList = new ArrayList<>();
        myList.add(new TranslationDetails("Traduit aussi cette phrase"));
        myList.add(new TranslationDetails("Je m'appelle Nicolas"));
        myList.add(new TranslationDetails("Isto é uma frase para teste"));
        myList = languageDetect(myList);
        translate( myList );
        System.out.println(myList);
    }

}
