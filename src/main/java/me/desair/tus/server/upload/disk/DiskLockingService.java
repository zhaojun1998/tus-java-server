package me.desair.tus.server.upload.disk;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import me.desair.tus.server.exception.TusException;
import me.desair.tus.server.exception.UploadAlreadyLockedException;
import me.desair.tus.server.upload.UploadId;
import me.desair.tus.server.upload.UploadIdFactory;
import me.desair.tus.server.upload.UploadLock;
import me.desair.tus.server.upload.UploadLockingService;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link UploadLockingService} implementation that uses the file system for implementing locking
 * <br>
 * File locking can also apply to shared network drives. This way the framework supports clustering
 * as long as the upload storage directory is mounted as a shared (network) drive. <br>
 * File locks are also automatically released on application (JVM) shutdown. This means the file
 * locking is not persistent and prevents cleanup and stale lock issues.
 */
public class DiskLockingService extends AbstractDiskBasedService implements UploadLockingService {

  private static final Logger log = LoggerFactory.getLogger(DiskLockingService.class);

  private static final String LOCK_SUB_DIRECTORY = "locks";

  private UploadIdFactory idFactory;

  /** Number of retry attempts when lock acquisition fails. Default is 0 (no retry). */
  private int lockRetryCount = 0;

  /** Initial retry interval in milliseconds. Default is 50ms. */
  private long lockRetryIntervalMs = 50;

  /** Maximum retry interval in milliseconds (for exponential backoff). Default is 500ms. */
  private long lockRetryMaxIntervalMs = 500;

  public DiskLockingService(String storagePath) {
    super(storagePath + File.separator + LOCK_SUB_DIRECTORY);
  }

  /** Constructor to use custom UploadIdFactory. */
  public DiskLockingService(UploadIdFactory idFactory, String storagePath) {
    this(storagePath);
    Validate.notNull(idFactory, "The IdFactory cannot be null");
    this.idFactory = idFactory;
  }

  /**
   * Constructor with lock retry configuration.
   *
   * <p>When a DELETE request is sent while a PATCH request is still in progress, the lock may not
   * be released yet. This constructor allows configuring retry behavior to handle such cases.
   *
   * @param storagePath The path where lock files are stored
   * @param lockRetryCount Number of retry attempts (0 means no retry)
   * @param lockRetryIntervalMs Initial retry interval in milliseconds
   * @param lockRetryMaxIntervalMs Maximum retry interval for exponential backoff
   */
  public DiskLockingService(
      String storagePath, int lockRetryCount, long lockRetryIntervalMs, long lockRetryMaxIntervalMs) {
    this(storagePath);
    this.lockRetryCount = lockRetryCount;
    this.lockRetryIntervalMs = lockRetryIntervalMs;
    this.lockRetryMaxIntervalMs = lockRetryMaxIntervalMs;
  }

  /**
   * Constructor with lock retry configuration and custom UploadIdFactory.
   *
   * @param idFactory The UploadIdFactory to use
   * @param storagePath The path where lock files are stored
   * @param lockRetryCount Number of retry attempts (0 means no retry)
   * @param lockRetryIntervalMs Initial retry interval in milliseconds
   * @param lockRetryMaxIntervalMs Maximum retry interval for exponential backoff
   */
  public DiskLockingService(
      UploadIdFactory idFactory,
      String storagePath,
      int lockRetryCount,
      long lockRetryIntervalMs,
      long lockRetryMaxIntervalMs) {
    this(storagePath, lockRetryCount, lockRetryIntervalMs, lockRetryMaxIntervalMs);
    Validate.notNull(idFactory, "The IdFactory cannot be null");
    this.idFactory = idFactory;
  }

  @Override
  public UploadLock lockUploadByUri(String requestUri) throws TusException, IOException {

    UploadId id = idFactory.readUploadId(requestUri);

    Path lockPath = getLockPath(id);
    // If lockPath is null, this is not a valid Upload URI
    if (lockPath == null) {
      return null;
    }

    // Try to acquire lock with optional retry
    UploadAlreadyLockedException lastException = null;
    long currentInterval = lockRetryIntervalMs;

    for (int attempt = 0; attempt <= lockRetryCount; attempt++) {
      try {
        return new FileBasedLock(requestUri, lockPath);
      } catch (UploadAlreadyLockedException e) {
        lastException = e;
        if (attempt < lockRetryCount) {
          log.info(
              "Lock acquisition failed, retrying in {}ms ({}/{}): {}",
              currentInterval,
              attempt + 1,
              lockRetryCount,
              requestUri);
          try {
            Thread.sleep(currentInterval);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw e;
          }
          // Exponential backoff with max interval
          currentInterval = Math.min(currentInterval * 2, lockRetryMaxIntervalMs);
        }
      }
    }

    if (lastException != null) {
      log.warn(
          "Lock acquisition failed after {} retries: {}", lockRetryCount, requestUri);
      throw lastException;
    }

    return null;
  }

  @Override
  public void cleanupStaleLocks() throws IOException {
    try (DirectoryStream<Path> locksStream = Files.newDirectoryStream(getStoragePath())) {
      for (Path path : locksStream) {

        FileTime lastModifiedTime = Files.getLastModifiedTime(path);
        if (lastModifiedTime.toMillis() < System.currentTimeMillis() - 10000L) {
          UploadId id = new UploadId(path.getFileName().toString());

          if (!isLocked(id)) {
            Files.deleteIfExists(path);
          }
        }
      }
    }
  }

  @Override
  public boolean isLocked(UploadId id) {
    boolean locked = false;
    Path lockPath = getLockPath(id);

    if (lockPath != null) {
      // Try to obtain a lock to see if the upload is currently locked
      try (UploadLock lock = new FileBasedLock(id.toString(), lockPath)) {

        // We got the lock, so it means no one else is locking it.
        locked = false;

      } catch (UploadAlreadyLockedException | IOException e) {
        // There was already a lock
        locked = true;
      }
    }

    return locked;
  }

  @Override
  public void setIdFactory(UploadIdFactory idFactory) {
    Validate.notNull(idFactory, "The IdFactory cannot be null");
    this.idFactory = idFactory;
  }

  private Path getLockPath(UploadId id) {
    return getPathInStorageDirectory(id);
  }
}
