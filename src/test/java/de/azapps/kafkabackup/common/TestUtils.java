package de.azapps.kafkabackup.common;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TestUtils {
    private static Path TEMP_DIR;

    static {
        try {
            TEMP_DIR = Files.createTempDirectory("kafka_backup_tests");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Path getTestDir(String tests) {
        Path ret = Paths.get(TEMP_DIR.toString(), tests);
        try {
            Files.createDirectories(ret);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return ret;
    }
}
