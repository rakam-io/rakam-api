import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;
import javax.xml.bind.DatatypeConverter;

public class ttes {
    public static void main(String[] args) {

    }

    public static String encrypt(String plainText, String encryptionKey) throws Exception {
        Cipher cipher = getCipher(Cipher.ENCRYPT_MODE, encryptionKey);
        byte[] encryptedBytes = cipher.doFinal(plainText.getBytes());

        return DatatypeConverter.printBase64Binary(encryptedBytes);
    }

    public static String decrypt(String encrypted, String encryptionKey) throws Exception {
        Cipher cipher = getCipher(Cipher.DECRYPT_MODE, encryptionKey);
        byte[] plainBytes = cipher.doFinal(DatatypeConverter.parseBase64Binary(encrypted));

        return new String(plainBytes);
    }

    private static Cipher getCipher(int cipherMode, String encryptionKey)
            throws Exception {
        String encryptionAlgorithm = "AES";
        SecretKeySpec keySpecification = new SecretKeySpec(
                encryptionKey.getBytes("UTF-8"), encryptionAlgorithm);
        Cipher cipher = Cipher.getInstance(encryptionAlgorithm);
        cipher.init(cipherMode, keySpecification);

        return cipher;
    }

    public static void main(String[] args) throws Exception {

        String encryptionKey = "MZygpewJsCpRrfOr";
        String plainText = "Hello world!";

        String cipherText = encrypt(plainText, encryptionKey);
        String decryptedCipherText = decrypt(cipherText, encryptionKey);
    }
}
