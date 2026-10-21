package me.desair.tus.server.upload.disk;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import me.desair.tus.server.upload.UploadId;
import me.desair.tus.server.upload.UploadInfo;
import me.desair.tus.server.upload.UploadLockingService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Directory stream filter that only accepts expired uploads that are not locked. An upload expires
 * when its expiration timestamp has passed and it is still in progress, or also when it is
 * completed if {@link DiskStorageService#isCompletedUploadExpirationEnabled()} is enabled. An
 * upload directory without readable upload information expires when nothing in it was modified
 * during the upload expiration period.
 */
public class ExpiredUploadFilter implements DirectoryStream.Filter<Path> {

  private static final Logger log = LoggerFactory.getLogger(ExpiredUploadFilter.class);

  private DiskStorageService diskStorageService;
  private UploadLockingService uploadLockingService;

  ExpiredUploadFilter(
      DiskStorageService diskStorageService, UploadLockingService uploadLockingService) {
    this.diskStorageService = diskStorageService;
    this.uploadLockingService = uploadLockingService;
  }

  @Override
  public boolean accept(Path upload) throws IOException {
    UploadId id = null;
    try {
      id = new UploadId(upload.getFileName().toString());
      if (isExpired(upload, id) && !uploadLockingService.isLocked(id)) {
        return true;
      }

    } catch (Exception ex) {
      if (log.isDebugEnabled()) {
        log.debug("Not able to determine state of upload " + Objects.toString(id), ex);
      }
    }

    return false;
  }

  /**
   * Check whether the upload stored in the given directory is expired, regardless of whether it is
   * locked.
   *
   * @param upload The directory of the upload
   * @param id The ID of the upload
   * @return True if the upload is expired
   * @throws IOException When the upload information or the directory cannot be read
   */
  boolean isExpired(Path upload, UploadId id) throws IOException {
    UploadInfo info = diskStorageService.getUploadInfo(id);
    if (info == null) {
      return isAbandoned(upload);
    }
    return info.isExpired()
        && (info.isUploadInProgress() || diskStorageService.isCompletedUploadExpirationEnabled());
  }

  /**
   * Upload information is missing when a crash interrupted the creation of an upload and is
   * unreadable when it was damaged, so the directory can only expire by its modification times.
   */
  private boolean isAbandoned(Path upload) throws IOException {
    Long expirationPeriod = diskStorageService.getUploadExpirationPeriod();
    if (expirationPeriod == null || expirationPeriod <= 0) {
      return false;
    }
    long lastModified = Files.getLastModifiedTime(upload).toMillis();
    try (DirectoryStream<Path> entries = Files.newDirectoryStream(upload)) {
      for (Path entry : entries) {
        lastModified = Math.max(lastModified, Files.getLastModifiedTime(entry).toMillis());
      }
    }
    return lastModified + expirationPeriod < System.currentTimeMillis();
  }
}
