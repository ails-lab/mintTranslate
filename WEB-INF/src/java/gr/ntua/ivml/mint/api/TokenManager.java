package gr.ntua.ivml.mint.api;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

import gr.ntua.ivml.mint.db.DB;
import gr.ntua.ivml.mint.persistent.User;
import gr.ntua.ivml.mint.util.Config;

public class TokenManager {
	
	static String getSecret() {
		return Config.get("secret");
	}

	static String encrypt(Long userId) throws Exception {
		long nowMillis = System.currentTimeMillis();
		// the token expires in 10 minutes
		long expires = nowMillis + 600000;
		String text = userId + ";" + expires;
		byte[] data = text.getBytes("UTF-8");
		SecretKey key = new SecretKeySpec(getSecret().getBytes("UTF-8"), "AES");
		Cipher cipher = Cipher.getInstance("AES/ECB/ISO10126PADDING");
		cipher.init(Cipher.ENCRYPT_MODE, key);
		byte[] result = cipher.doFinal(data);
		String res = DatatypeConverter.printHexBinary(result);
		return (res);
	}
	
	static String decrypt(String token) throws Exception {
		byte[] cipherData = DatatypeConverter.parseHexBinary(token);
		Cipher cipher = Cipher.getInstance("AES/ECB/ISO10126PADDING");
		SecretKey key = new SecretKeySpec(getSecret().getBytes("UTF-8"), "AES");
		cipher.init(Cipher.DECRYPT_MODE, key);
		String plain = new String(cipher.doFinal(cipherData), "UTF-8");
		return plain;
	}

	static boolean isValidToken(String token) {
		try {
			return (getUserFromToken(token) != null);
		} catch (Exception e) {
			return false;
		}
	}
	
	static User getUserFromToken(String token) throws Exception {
		String plain = decrypt(token);
		String[] tokenInfo = plain.split(";");
		if (tokenInfo.length < 2)
			throw new Exception();
		Long userId = Long.parseLong(tokenInfo[0]);
		Long expires = Long.parseLong(tokenInfo[1]);
		if (expires < System.currentTimeMillis())
			throw new Exception();
		return DB.getUserDAO().getById(userId, false);
	}
}
