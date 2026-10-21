package me.desair.tus.server.upload;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.util.UUID;
import org.junit.Test;

public class UploadLockingServiceTest {

  @Test
  public void testDefaultMethods() {
    UploadLockingService service =
        new UploadLockingService() {
          @Override
          public UploadLock lockUploadByUri(String requestUri) {
            return null;
          }

          @Override
          public void cleanupStaleLocks() {}

          @Override
          public boolean isLocked(UploadId id) {
            return false;
          }

          @Override
          public void setIdFactory(UploadIdFactory idFactory) {}
        };

    // Verify default methods do not throw exceptions and act as no-ops
    service.registerInputStream("/files/test", new ByteArrayInputStream(new byte[0]));
    service.requestLockRelease("/files/test");
    assertNotNull(service);
  }

  @Test
  public void testDefaultLockUploadIsUnsupported() throws Exception {
    UploadLockingService service =
        new UploadLockingService() {
          @Override
          public UploadLock lockUploadByUri(String requestUri) {
            return null;
          }

          @Override
          public void cleanupStaleLocks() {}

          @Override
          public boolean isLocked(UploadId id) {
            return false;
          }

          @Override
          public void setIdFactory(UploadIdFactory idFactory) {}
        };

    assertNull(service.lockUpload(new UploadId(UUID.randomUUID())));
  }
}
