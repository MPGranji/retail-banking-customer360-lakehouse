import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.SecureRandom;
import java.util.HexFormat;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

/** Generates Trino-compatible PBKDF2 records without storing plaintext passwords. */
public final class GeneratePasswordFile {
    private static final int ITERATIONS = 100_000;
    private static final int SALT_BYTES = 16;
    private static final int HASH_BITS = 512;

    private GeneratePasswordFile() {}

    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new IllegalArgumentException("Usage: output");
        }

        String[][] credentials = {
                {"marketing", requireEnvironment("TRINO_MARKETING_PASSWORD")},
                {"data_engineer", requireEnvironment("TRINO_ENGINEERING_PASSWORD")},
        };
        var random = new SecureRandom();
        var output = new StringBuilder();
        for (var credential : credentials) {
            var salt = new byte[SALT_BYTES];
            random.nextBytes(salt);
            var spec = new PBEKeySpec(credential[1].toCharArray(), salt, ITERATIONS, HASH_BITS);
            var hash = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA1").generateSecret(spec).getEncoded();
            output.append(credential[0]).append(':')
                    .append(ITERATIONS).append(':')
                    .append(HexFormat.of().formatHex(salt)).append(':')
                    .append(HexFormat.of().formatHex(hash)).append('\n');
            spec.clearPassword();
        }

        var destination = Path.of(args[0]);
        var temporary = destination.resolveSibling(destination.getFileName() + ".tmp");
        Files.writeString(temporary, output.toString(), StandardCharsets.UTF_8);
        Files.move(temporary, destination, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
    }

    private static String requireEnvironment(String name) {
        var value = System.getenv(name);
        if (value == null || value.isBlank() || value.equals("CHANGE_ME")) {
            throw new IllegalArgumentException(name + " must be configured");
        }
        return value;
    }
}
