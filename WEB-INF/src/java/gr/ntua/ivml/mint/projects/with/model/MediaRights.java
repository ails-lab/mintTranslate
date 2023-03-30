package gr.ntua.ivml.mint.projects.with.model;


public enum MediaRights {
	Public("CC0 Public domain"), Restricted("Restricted"), Permission(
			"Permission"), Modify("Allow re-use and modifications"), Commercial(
			"Allow re-use for commercial"),
	/*
	 * Creative_Commercial_Modify(
	 * "use for commercial purposes modify, adapt, or build upon"),
	 * Creative_Not_Commercial("NOT Comercial"),
	 * Creative_Not_Modify("NOT Modify"), Creative_Not_Commercial_Modify(
	 * "not modify, adapt, or build upon, not for commercial purposes"),
	 * Creative_SA("share alike"), Creative_BY("use by attribution"),
	 * Creative("Allow re-use"),
	 */
	Creative_Not_Commercial_Share_Alike(
			"Creative_BY_NC_SA"),
	
	Creative_BY_NC_SA("Attribution NonCommercial-ShareAlike" ),
	// CC-BY-NC-SA

	// Creative_Not_Commercial_Modify(
	// "use for commercial purposes modify, adapt, or build upon"),

	Creative_Not_Commercial("NOT Comercial"),
	// CC-BY-NC
	Creative_Not_Modify("NOT Modify"),
	// CC-BY-ND
	Creative_Not_Commercial_Modify(
			"not modify, adapt, or build upon, not for commercial purposes"),
	// CC-BY-NC-ND
	Creative_SA("share alike"),
	// CC-BY-SA
	Creative_BY("use by attribution"),
	// CC-BY
	Creative("Creative"),
	// CC0?
	RR("Rights Reserved"), RRPA("Rights Reserved - Paid Access"), RRRA(
			"Rights Reserved - Restricted Access"), RRFA(
			"Rights Reserved - Free Access"), UNKNOWN("UNKNOWN");

/*
 * The Public Domain Mark (PDM), No Rights Reserved CC0, Attribution, Attribution ShareAlike, 
 *  Attribution NonCommercial, Attribution NoDerivs, Attribution NonCommercial-ShareAlike, Attribution NonCommercial-NoDerivs, 
 *  In Copyright (InC), IN COPYRIGHT - EU ORPHAN WORK, IN COPYRIGHT - EDUCATIONAL USE PERMITTED, 
 *  IN COPYRIGHT - NON-COMMERCIAL USE PERMITTED, IN COPYRIGHT - RIGHTS-HOLDER(S) UNLOCATABLE OR UNIDENTIFIABLE, 
 *  NO COPYRIGHT - CONTRACTUAL RESTRICTIONS, NO COPYRIGHT - NON-COMMERCIAL USE ONLY, 
 *  NO COPYRIGHT - OTHER KNOWN LEGAL RESTRICTIONS, NO COPYRIGHT - UNITED STATES, COPYRIGHT NOT EVALUATED, COPYRIGHT UNDETERMINED, 
 *  NO KNOWN COPYRIGHT, In Copyright (InC), Out of Copyright, Restricted, Rights Reserved - Paid Access, 
 *  Rights Reserved - Restricted Access, Rights Reserved - Free Access, Provider specific rights statement, 
 *  Modify, Creative SA, Permission granted, NOT comercial modify, Creative, Not Modify, Not Comercial
 * 
 * 
 */
 
	
	
	private final String text;

	private MediaRights(final String text) {
		this.text = text;
	}

	@Override
	public String toString() {
		return name();
	}

	public static MediaRights fromString( String original) {
		MediaRights wRights = MediaRights.UNKNOWN;
		
		if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(original,
				"creative")) {

			if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(
					original, "by-nc-nd")) {
				wRights = MediaRights.Creative_Not_Commercial_Modify;
			}

			else if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(
					original, "by-nc-sa")) {
				wRights = MediaRights.Creative_BY_NC_SA;
			}

			else if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(
					original, "by-nc")) {
				wRights = MediaRights.Creative_Not_Commercial;

			}

			else if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(
					original, "by-nd")) {
				wRights = MediaRights.Creative_Not_Modify;

			}

			else if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(
					original, "by-sa")) {
				wRights = MediaRights.Creative_SA;

			}

			else if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(
					original, "by")) {
				wRights = MediaRights.Creative_BY;

			} else if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(
					original, "cc0")
					|| org.apache.commons.lang3.StringUtils.containsIgnoreCase(
							original, "publicdomain")) {
				wRights = MediaRights.Creative;

			}

		}

		else if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(
				original, "rights")) {
			if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(
					original, "rr-f")) {
				wRights = MediaRights.RRFA;

			}

			else if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(
					original, "rr-r")) {
				wRights = MediaRights.RRRA;

			}

			else if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(
					original, "rr-p")) {
				wRights = MediaRights.RRPA;

			}

			else if (org.apache.commons.lang3.StringUtils.containsIgnoreCase(
					original, "rr")) {
				wRights = MediaRights.RR;
			}

		}

		return  wRights;

	}
}
