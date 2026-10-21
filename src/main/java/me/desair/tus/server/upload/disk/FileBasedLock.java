package me.desair.tus.server.upload.disk;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import me.desair.tus.server.exception.UploadAlreadyLockedException;
import me.desair.tus.server.upload.UploadLock;
import me.desair.tus.server.util.Utils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Upload locking implementation using the file system file locking mechanism. File locking can also
 * apply to shared network drives. This way the framework supports clustering as long as the upload
 * storage directory is mounted as a shared (network) drive. <br>
 * File locks are also automatically released on application (JVM) shutdown. This means the file
 * locking is not persistent and prevents cleanup and stale lock issues.
 *
 * <p>Lock files are deleted on release. A request may open the lock file just before the previous
 * holder unlinks it and acquire the OS lock on that unlinked file afterwards, while a third request
 * creates and locks a new file at the same path. To keep the lock exclusive, every holder writes a
 * random token into the file it locked and checks that the lock path still contains that token; if
 * not, the locked file is stale and acquisition is retried on the current file. A holder only
 * deletes the lock path while it still owns it, and does so before releasing the OS lock.
 */
public class FileBasedLock implements UploadLock {

  private static final Logger log = LoggerFactory.getLogger(FileBasedLock.class);

  /** Acquisition attempts when the locked file turns out to be unlinked by a previous holder. */
  private static final int MAX_LOCK_ATTEMPTS = 3;

  private String uploadUri;
  private FileChannel fileChannel = null;

  protected Path lockPath;
  private final byte[] lockToken = UUID.randomUUID().toString().getBytes(StandardCharsets.US_ASCII);

  /** Constructor. */
  public FileBasedLock(String uploadUri, Path lockPath)
      throws UploadAlreadyLockedException, IOException {
    Validate.notBlank(uploadUri, "The upload URI cannot be blank");
    Objects.requireNonNull(lockPath, "The path to the lock cannot be null");
    this.uploadUri = uploadUri;
    this.lockPath = lockPath;

    tryToObtainFileLock();
  }

  private void tryToObtainFileLock() throws UploadAlreadyLockedException, IOException {
    for (int attempt = 1; attempt <= MAX_LOCK_ATTEMPTS; attempt++) {
      FileChannel channel = openLockedChannel();
      try {
        channel.truncate(0);
        channel.write(ByteBuffer.wrap(lockToken), 0);
      } catch (IOException e) {
        closeQuietly(channel);
        throw new IOException("Unable to write the lock file for URI " + getUploadUri(), e);
      }
      if (ownsLockPath()) {
        fileChannel = channel;
        return;
      }
      closeQuietly(channel);
    }
    throw new UploadAlreadyLockedException(alreadyLockedMessage());
  }

  private FileChannel openLockedChannel() throws UploadAlreadyLockedException, IOException {
    FileChannel channel = null;
    try {
      channel = createFileChannel();
      // If the upload is already locked by another process, our lock will be null
      if (Utils.lockFileExclusively(channel) == null) {
        closeQuietly(channel);
        throw new UploadAlreadyLockedException(alreadyLockedMessage());
      }
      return channel;
    } catch (OverlappingFileLockException e) {
      closeQuietly(channel);
      throw new UploadAlreadyLockedException(alreadyLockedMessage());
    } catch (IOException e) {
      closeQuietly(channel);
      throw new IOException(
          "Unable to create or open file required to implement file-based locking", e);
    }
  }

  private boolean ownsLockPath() {
    try {
      return Arrays.equals(Files.readAllBytes(lockPath), lockToken);
    } catch (IOException e) {
      // The path was deleted or replaced by another holder
      return false;
    }
  }

  private String alreadyLockedMessage() {
    return "The upload " + getUploadUri() + " is already locked";
  }

  private void closeQuietly(FileChannel channel) {
    if (channel != null) {
      try {
        channel.close();
      } catch (IOException e) {
        log.warn("Unable to close lock file channel for URI " + getUploadUri(), e);
      }
    }
  }

  @Override
  public String getUploadUri() {
    return uploadUri;
  }

  public Path getLockPath() {
    return lockPath;
  }

  @Override
  public void release() {
    try {
      if (ownsLockPath()) {
        Files.deleteIfExists(lockPath);
      }
    } catch (IOException e) {
      log.warn("Unable to delete lock file for URI " + getUploadUri(), e);
    } finally {
      // Closing the channel will also release the lock
      closeQuietly(fileChannel);
    }
  }

  @Override
  public void close() throws IOException {
    release();
  }

  protected FileChannel createFileChannel() throws IOException {
    return FileChannel.open(lockPath, CREATE, WRITE);
  }
}
