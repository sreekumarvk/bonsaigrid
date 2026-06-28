package com.bonsaigrid;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class BonsaiGrid {

    static {
        try {
            // Determine OS and architecture to load the correct library
            String os = System.getProperty("os.name").toLowerCase();
            String libName = "libbonsaigrid_jni.so"; // Default to Linux

            if (os.contains("win")) {
                libName = "bonsaigrid_jni.dll";
            } else if (os.contains("mac")) {
                libName = "libbonsaigrid_jni.dylib";
            }

            // Extract the library from the JAR to a temporary location
            InputStream is = BonsaiGrid.class.getResourceAsStream("/" + libName);
            if (is != null) {
                Path tempFile = Files.createTempFile("bonsaigrid_jni", null);
                Files.copy(is, tempFile, StandardCopyOption.REPLACE_EXISTING);
                System.load(tempFile.toAbsolutePath().toString());
                tempFile.toFile().deleteOnExit();
            } else {
                // Fallback for local development (if not running from JAR)
                System.loadLibrary("bonsaigrid_jni");
            }
        } catch (Exception e) {
            System.err.println("Failed to load BonsaiGrid native library: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Starts the embedded BonsaiGrid server asynchronously.
     * The server binds to localhost on the standard Hazelcast port (5701).
     */
    public static native void startServer();

    // Small test main method
    public static void main(String[] args) throws InterruptedException {
        System.out.println("Starting embedded BonsaiGrid server...");
        BonsaiGrid.startServer();
        System.out.println("Server started in the background. Press Ctrl+C to exit.");
        
        // Keep the JVM alive
        Thread.sleep(Long.MAX_VALUE);
    }
}
