package me.desair.tus.server.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import org.apache.commons.io.FileUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class UtilsTest {

  private static Path storagePath;

  @BeforeClass
  public static void setupDataFolder() throws IOException {
    storagePath = Paths.get("target", "tus", "utils-test").toAbsolutePath();
    Files.createDirectories(storagePath);
  }

  @AfterClass
  public static void destroyDataFolder() throws IOException {
    FileUtils.deleteDirectory(storagePath.toFile());
  }

  @Test
  public void readSerializableWithValidFile() throws Exception {
    Path testFile = storagePath.resolve("valid-" + UUID.randomUUID());
    TestSerializable original = new TestSerializable("test-value");

    Utils.writeSerializable(original, testFile);

    TestSerializable result = Utils.readSerializable(testFile, TestSerializable.class);

    assertThat(result.getValue(), is("test-value"));

    Files.deleteIfExists(testFile);
  }

  @Test
  public void readSerializableWithCorruptedFile() throws Exception {
    Path corruptedFile = storagePath.resolve("corrupted-" + UUID.randomUUID());

    // Create a corrupted file with invalid serialization data
    Files.write(corruptedFile, "this is not valid serialized data".getBytes());

    // Should return null instead of throwing an exception
    TestSerializable result = Utils.readSerializable(corruptedFile, TestSerializable.class);

    assertThat(result, is(nullValue()));

    Files.deleteIfExists(corruptedFile);
  }

  @Test
  public void readSerializableWithTruncatedFile() throws Exception {
    Path truncatedFile = storagePath.resolve("truncated-" + UUID.randomUUID());

    // Create a truncated file (partial serialization header)
    // Java serialization magic number is 0xACED, followed by version
    Files.write(truncatedFile, new byte[] {(byte) 0xAC, (byte) 0xED, 0x00});

    // Should return null instead of throwing EOFException
    TestSerializable result = Utils.readSerializable(truncatedFile, TestSerializable.class);

    assertThat(result, is(nullValue()));

    Files.deleteIfExists(truncatedFile);
  }

  @Test
  public void readSerializableWithEmptyFile() throws Exception {
    Path emptyFile = storagePath.resolve("empty-" + UUID.randomUUID());

    // Create an empty file
    Files.createFile(emptyFile);

    // Should return null instead of throwing EOFException
    TestSerializable result = Utils.readSerializable(emptyFile, TestSerializable.class);

    assertThat(result, is(nullValue()));

    Files.deleteIfExists(emptyFile);
  }

  @Test
  public void readSerializableWithNullPath() throws Exception {
    // Should return null when path is null
    TestSerializable result = Utils.readSerializable(null, TestSerializable.class);

    assertThat(result, is(nullValue()));
  }

  /** Simple serializable class for testing. */
  public static class TestSerializable implements Serializable {
    private static final long serialVersionUID = 1L;
    private final String value;

    public TestSerializable(String value) {
      this.value = value;
    }

    public String getValue() {
      return value;
    }
  }
}
