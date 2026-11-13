package me.desair.tus.server.util;

import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InvalidClassException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Pattern;
import me.desair.tus.server.HttpHeader;
import me.desair.tus.server.checksum.ChecksumAlgorithm;
import me.desair.tus.server.upload.UploadId;
import me.desair.tus.server.upload.UploadInfo;
import me.desair.tus.server.upload.UploadType;
import org.apache.commons.io.serialization.ValidatingObjectInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class that contains various static helper methods */
public class Utils {

  private static final Logger log = LoggerFactory.getLogger(Utils.class);
  private static final int LOCK_FILE_RETRY_COUNT = 3;
  private static final long LOCK_FILE_SLEEP_TIME = 500;
  private static final Pattern CHECKSUM_VALUE_PATTERN = Pattern.compile("^[a-zA-Z0-9+/=\\-_]+$");

  private Utils() {
    // This is a utility class that only holds static utility methods
  }

  public static String getHeader(HttpServletRequest request, String header) {
    return StringUtils.trimToEmpty(request.getHeader(header));
  }

  /** Matches the base media type without parameters, ignoring case per RFC 9110 section 8.3. */
  public static boolean isMediaType(String contentTypeHeader, String expectedMediaType) {
    if (contentTypeHeader == null || expectedMediaType == null) {
      return false;
    }
    int semicolon = contentTypeHeader.indexOf(';');
    String baseType =
        semicolon >= 0 ? contentTypeHeader.substring(0, semicolon) : contentTypeHeader;
    return Strings.CI.equals(baseType.trim(), expectedMediaType.trim());
  }

  public static Long getLongHeader(HttpServletRequest request, String header) {
    try {
      return Long.valueOf(getHeader(request, header));
    } catch (NumberFormatException ex) {
      return null;
    }
  }

  /**
   * Build a comma-separated list based on the remote address of the request and the
   * X-Forwareded-For header. The list is constructed as "client, proxy1, proxy2".
   *
   * @return A comma-separated list of ip-addresses
   */
  public static String buildRemoteIpList(HttpServletRequest servletRequest) {
    String ipAddresses = servletRequest.getRemoteAddr();
    String xforwardedForHeader = getHeader(servletRequest, HttpHeader.X_FORWARDED_FOR);
    if (xforwardedForHeader.length() > 0) {
      ipAddresses = xforwardedForHeader + ", " + ipAddresses;
    }
    return ipAddresses;
  }

  public static List<String> parseConcatenationIDsFromHeader(String uploadConcatValue) {
    List<String> output = new LinkedList<>();

    String idString = StringUtils.substringAfter(uploadConcatValue, ";");
    for (String id : StringUtils.trimToEmpty(idString).split("\\s")) {
      output.add(id);
    }

    return output;
  }

  /**
   * Read a serialized object from the given path.
   *
   * <p>No file lock is taken: {@link #writeSerializable(Serializable, Path)} replaces the file
   * atomically, so a reader always sees either the previous or the new complete content.
   *
   * <p>Only the requested root type and explicitly allowed upload metadata types are deserialized;
   * arbitrary classes in their packages are not trusted.
   *
   * @param path The file to read, or null
   * @param clazz The expected type of the serialized object
   * @return The deserialized object, or null if the path is null, the file does not exist or its
   *     content is truncated, corrupted or contains a disallowed class
   * @throws IOException When the file cannot be read
   */
  public static <T> T readSerializable(Path path, Class<T> clazz) throws IOException {
    T info = null;
    if (path != null && Files.exists(path)) {
      try (InputStream is = Files.newInputStream(path);
          ValidatingObjectInputStream ois = new ValidatingObjectInputStream(is)) {
        ois.accept(
            clazz,
            UploadInfo.class,
            UploadId.class,
            UploadType.class,
            ChecksumAlgorithm.class,
            String.class,
            Long.class,
            Integer.class,
            Boolean.class,
            Number.class,
            Object.class,
            Enum.class,
            Date.class,
            UUID.class,
            ArrayList.class,
            LinkedList.class,
            TreeMap.class,
            HashMap.class,
            HashSet.class,
            TreeSet.class,
            String[].class,
            Object[].class,
            Arrays.asList().getClass(),
            Collections.unmodifiableList(new ArrayList<>()).getClass(),
            // Unmodifiable lists serialize their non-random-access form and collection superclass.
            Collections.unmodifiableList(new LinkedList<>()).getClass(),
            Collections.unmodifiableCollection(new ArrayList<>()).getClass(),
            Collections.emptyList().getClass());
        info = clazz.cast(ois.readObject());
      } catch (ClassNotFoundException
          | InvalidClassException
          | java.io.EOFException
          | java.io.StreamCorruptedException e) {
        // A failed write or a disallowed serialized type makes the metadata unusable.
        log.warn("Unable to read serializable file {}: {}", path, e.getMessage());
        info = null;
      }
    }
    return info;
  }

  /**
   * Serialize an object to the given path.
   *
   * <p>The object is first written to a temporary sibling file which then atomically replaces the
   * target. Truncating and locking the target in place would let a concurrent reader in the same
   * JVM make the lock attempt fail with an {@link java.nio.channels.OverlappingFileLockException}
   * after the file was already truncated, and a crash during the write would leave a partial file.
   *
   * @param object The object to serialize
   * @param path The target file, or null to do nothing
   * @throws IOException When the temporary file cannot be written or moved into place
   */
  public static void writeSerializable(Serializable object, Path path) throws IOException {
    if (path != null) {
      Path tempPath = path.resolveSibling(path.getFileName() + "." + UUID.randomUUID() + ".tmp");
      try {
        try (OutputStream buffer =
                new BufferedOutputStream(
                    Files.newOutputStream(tempPath, WRITE, CREATE, TRUNCATE_EXISTING));
            ObjectOutput output = new ObjectOutputStream(buffer)) {

          output.writeObject(object);
        }
        // The temporary file is a sibling of the target, so both are on the same file store
        Files.move(tempPath, path, ATOMIC_MOVE, REPLACE_EXISTING);
      } finally {
        Files.deleteIfExists(tempPath);
      }
    }
  }

  public static FileLock lockFileExclusively(FileChannel channel) throws IOException {
    return lockFile(channel, 0L, Long.MAX_VALUE, false);
  }

  /** Locks an exclusive byte range while preserving the standard lock retry policy. */
  public static FileLock lockFileExclusively(FileChannel channel, long position, long size)
      throws IOException {
    return lockFile(channel, position, size, false);
  }

  public static FileLock lockFileShared(FileChannel channel) throws IOException {
    return lockFile(channel, 0L, Long.MAX_VALUE, true);
  }

  /**
   * Sleep the specified number of milliseconds
   *
   * @param sleepTimeMillis The time to sleep in milliseconds
   */
  public static void sleep(long sleepTimeMillis) {
    try {
      Thread.sleep(sleepTimeMillis);
    } catch (InterruptedException e) {
      log.warn("Sleep was interrupted");
      // Restore interrupted state...
      Thread.currentThread().interrupt();
    }
  }

  private static FileLock lockFile(FileChannel channel, long position, long size, boolean shared)
      throws IOException {
    int i = 0;
    FileLock lock = null;
    do {
      if (i > 0) {
        sleep(LOCK_FILE_SLEEP_TIME);
      }

      lock = channel.tryLock(position, size, shared);

      i++;
    } while (lock == null && i < LOCK_FILE_RETRY_COUNT);

    return lock;
  }

  /** Helper class to store parsed checksum header information. */
  public static class ChecksumInfo {
    private final ChecksumAlgorithm algorithm;
    private final String value;

    public ChecksumInfo(ChecksumAlgorithm algorithm, String value) {
      this.algorithm = algorithm;
      this.value = value;
    }

    public ChecksumAlgorithm getAlgorithm() {
      return algorithm;
    }

    public String getValue() {
      return value;
    }
  }

  /**
   * Parse the Upload-Checksum header from the HTTP request.
   *
   * @param request The HttpServletRequest
   * @return ChecksumInfo if header is present and valid, null otherwise
   */
  public static ChecksumInfo parseUploadChecksumHeader(HttpServletRequest request) {
    String uploadChecksumHeader = request.getHeader(HttpHeader.UPLOAD_CHECKSUM);
    if (StringUtils.isNotBlank(uploadChecksumHeader)) {
      ChecksumAlgorithm algorithm = ChecksumAlgorithm.forUploadChecksumHeader(uploadChecksumHeader);
      String checksumValue =
          StringUtils.substringAfter(
              uploadChecksumHeader, ChecksumAlgorithm.CHECKSUM_VALUE_SEPARATOR);
      if (algorithm != null
          && StringUtils.isNotBlank(checksumValue)
          && CHECKSUM_VALUE_PATTERN.matcher(checksumValue).matches()) {
        return new ChecksumInfo(algorithm, checksumValue);
      }
    }
    return null;
  }

  public static String getUploadURI(HttpServletRequest request, HttpServletResponse response) {
    String location = response.getHeader(HttpHeader.LOCATION);
    return StringUtils.isNotBlank(location) ? location : request.getRequestURI();
  }
}
