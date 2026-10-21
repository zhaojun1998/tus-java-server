package me.desair.tus.server;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.time.Duration;
import me.desair.tus.server.exception.UploadAlreadyLockedException;
import me.desair.tus.server.upload.UploadLock;
import me.desair.tus.server.upload.UploadLockingService;
import me.desair.tus.server.upload.UploadStorageService;
import org.junit.Test;

public class TusFileUploadServiceTest {

  @Test
  public void testAcquireUploadLockInterrupted() throws Exception {
    UploadLockingService mockLockingService = mock(UploadLockingService.class);
    when(mockLockingService.lockUploadByUri(anyString()))
        .thenThrow(new UploadAlreadyLockedException("Locked"));

    TusFileUploadService service =
        new TusFileUploadService().withUploadLockingService(mockLockingService);

    // Interrupt the thread to trigger InterruptedException during sleep
    Thread.currentThread().interrupt();

    try {
      service.acquireUploadLock(HttpMethod.HEAD, "/files/test");
      fail("Expected IOException due to thread interruption");
    } catch (IOException e) {
      // Clear interrupted flag so it doesn't leak to other tests
      Thread.interrupted();
      assertNotNull(e.getCause());
    }
  }

  @Test
  public void testAcquireUploadLockFallback() throws Exception {
    UploadLockingService mockLockingService = mock(UploadLockingService.class);
    UploadLock mockLock = mock(UploadLock.class);

    // We throw exception 25 times and then succeed.
    // To avoid waiting 5 seconds (25 * 200ms) in the test, we mock Thread.sleep by interrupting
    // inside the mock,
    // but wait, mockLockingService doesn't run sleep. Sleep runs in the service itself.
    // Instead of doing 25 times which takes 5 seconds, let's just do it. 5 seconds is perfectly
    // fine for a fallback test.
    var stubbing = when(mockLockingService.lockUploadByUri(anyString()));
    for (int i = 0; i < 25; i++) {
      stubbing = stubbing.thenThrow(new UploadAlreadyLockedException("Locked"));
    }
    stubbing.thenReturn(mockLock);

    TusFileUploadService service =
        new TusFileUploadService().withUploadLockingService(mockLockingService);

    UploadLock lock = service.acquireUploadLock(HttpMethod.HEAD, "/files/test");
    assertNotNull(lock);
  }

  @Test
  public void testAcquireUploadLockPatchThrowsImmediately() throws Exception {
    UploadLockingService mockLockingService = mock(UploadLockingService.class);
    when(mockLockingService.lockUploadByUri(anyString()))
        .thenThrow(new UploadAlreadyLockedException("Locked"));

    TusFileUploadService service =
        new TusFileUploadService().withUploadLockingService(mockLockingService);

    try {
      service.acquireUploadLock(HttpMethod.PATCH, "/files/test");
      fail("Expected UploadAlreadyLockedException");
    } catch (UploadAlreadyLockedException e) {
      // expected
    }
  }

  @Test
  public void testAcquireUploadLockDeleteInterrupted() throws Exception {
    UploadLockingService mockLockingService = mock(UploadLockingService.class);
    when(mockLockingService.lockUploadByUri(anyString()))
        .thenThrow(new UploadAlreadyLockedException("Locked"));

    TusFileUploadService service =
        new TusFileUploadService().withUploadLockingService(mockLockingService);

    // Interrupt the thread to trigger InterruptedException during sleep
    Thread.currentThread().interrupt();

    try {
      service.acquireUploadLock(HttpMethod.DELETE, "/files/test");
      fail("Expected IOException due to thread interruption");
    } catch (IOException e) {
      // Clear interrupted flag so it doesn't leak to other tests
      Thread.interrupted();
      assertNotNull(e.getCause());
    }
  }

  @Test
  public void testAcquireUploadLockDeleteFallback() throws Exception {
    UploadLockingService mockLockingService = mock(UploadLockingService.class);
    UploadLock mockLock = mock(UploadLock.class);

    var stubbing = when(mockLockingService.lockUploadByUri(anyString()));
    for (int i = 0; i < 25; i++) {
      stubbing = stubbing.thenThrow(new UploadAlreadyLockedException("Locked"));
    }
    stubbing.thenReturn(mockLock);

    TusFileUploadService service =
        new TusFileUploadService().withUploadLockingService(mockLockingService);

    UploadLock lock = service.acquireUploadLock(HttpMethod.DELETE, "/files/test");
    assertNotNull(lock);
  }

  @Test
  public void testProcessSuccess() throws Exception {
    UploadLockingService mockLockingService = mock(UploadLockingService.class);
    UploadLock mockLock = mock(UploadLock.class);
    when(mockLockingService.lockUploadByUri(anyString())).thenReturn(mockLock);

    jakarta.servlet.http.HttpServletRequest mockReq =
        mock(jakarta.servlet.http.HttpServletRequest.class);
    jakarta.servlet.http.HttpServletResponse mockResp =
        mock(jakarta.servlet.http.HttpServletResponse.class);
    when(mockReq.getMethod()).thenReturn("PATCH");
    when(mockReq.getRequestURI()).thenReturn("/files/test");
    when(mockReq.getHeader(anyString())).thenReturn("");

    TusFileUploadService service =
        new TusFileUploadService().withUploadLockingService(mockLockingService);

    me.desair.tus.server.upload.UploadStorageService mockStorage =
        mock(me.desair.tus.server.upload.UploadStorageService.class);
    service.withUploadStorageService(mockStorage);
    when(mockStorage.getUploadInfo(anyString(), anyString())).thenReturn(null);

    service.process(mockReq, mockResp, "owner");

    verify(mockLockingService, times(1)).lockUploadByUri("/files/test");
    verify(mockLock, times(1)).close();
  }

  @Test
  public void testProcessLockFailure() throws Exception {
    UploadLockingService mockLockingService = mock(UploadLockingService.class);
    when(mockLockingService.lockUploadByUri(anyString()))
        .thenThrow(new UploadAlreadyLockedException("Locked"));

    jakarta.servlet.http.HttpServletRequest mockReq =
        mock(jakarta.servlet.http.HttpServletRequest.class);
    jakarta.servlet.http.HttpServletResponse mockResp =
        mock(jakarta.servlet.http.HttpServletResponse.class);
    when(mockReq.getMethod()).thenReturn("PATCH");
    when(mockReq.getRequestURI()).thenReturn("/files/test");

    TusFileUploadService service =
        new TusFileUploadService().withUploadLockingService(mockLockingService);

    service.process(mockReq, mockResp, "owner");

    verify(mockResp, times(1)).sendError(423, "Locked");
  }

  @Test
  public void testDisableCreationWithUploadWhenCreationDisabled() throws Exception {
    TusFileUploadService service = new TusFileUploadService();
    // Disable creation extension first so it is not present
    service.disableTusExtension("creation");

    // Disable creation-with-upload should not fail or throw ClassCastException/NullPointerException
    try {
      service.disableTusExtension("creation-with-upload");
    } catch (Exception e) {
      fail(
          "Should not throw exception when disabling creation-with-upload when creation is not enabled");
    }
  }

  @Test
  public void testStorageSettingsAreKeptWhenStorageServiceIsReplaced() {
    UploadStorageService storageService = mock(UploadStorageService.class);

    new TusFileUploadService()
        .withUploadDataSync(false)
        .withCompletedUploadExpiration(true)
        .withUploadStorageService(storageService);

    verify(storageService).setUploadDataSyncEnabled(false);
    verify(storageService).setCompletedUploadExpirationEnabled(true);
  }

  @Test
  public void testUploadAccessFailsImmediatelyWhenLockedByDefault() throws Exception {
    UploadLockingService lockingService = mock(UploadLockingService.class);
    when(lockingService.lockUploadByUri("/files/test"))
        .thenThrow(new UploadAlreadyLockedException("Locked"));
    TusFileUploadService service =
        new TusFileUploadService()
            .withUploadStorageService(mock(UploadStorageService.class))
            .withUploadLockingService(lockingService);

    assertThrows(
        UploadAlreadyLockedException.class, () -> service.getUploadInfo("/files/test", "owner"));
    verify(lockingService, times(1)).lockUploadByUri("/files/test");
  }

  @Test
  public void testUploadAccessWaitsForLockWithoutInterruptingTheHolder() throws Exception {
    UploadLockingService lockingService = mock(UploadLockingService.class);
    UploadLock lock = mock(UploadLock.class);
    UploadAlreadyLockedException locked = new UploadAlreadyLockedException("Locked");
    when(lockingService.lockUploadByUri("/files/test"))
        .thenThrow(locked)
        .thenReturn(lock)
        .thenThrow(locked)
        .thenReturn(lock)
        .thenThrow(locked)
        .thenReturn(lock)
        .thenThrow(locked)
        .thenReturn(lock);
    TusFileUploadService service =
        new TusFileUploadService()
            .withUploadStorageService(mock(UploadStorageService.class))
            .withUploadLockingService(lockingService)
            .withUploadLockWaitTimeout(Duration.ofSeconds(5));

    service.getUploadInfo("/files/test", "owner");
    service.getUploadedBytes("/files/test", "owner");
    service.getUploadedFile("/files/test", "owner");
    service.deleteUpload("/files/test", "owner");

    verify(lock, times(4)).close();
    verify(lockingService, never()).requestLockRelease(anyString());
  }

  @Test
  public void testUploadAccessFailsWhenLockWaitTimeoutExpires() throws Exception {
    UploadLockingService lockingService = mock(UploadLockingService.class);
    when(lockingService.lockUploadByUri("/files/test"))
        .thenThrow(new UploadAlreadyLockedException("Locked"));
    TusFileUploadService service =
        new TusFileUploadService()
            .withUploadStorageService(mock(UploadStorageService.class))
            .withUploadLockingService(lockingService)
            .withUploadLockWaitTimeout(Duration.ofMillis(500));

    long start = System.nanoTime();
    assertThrows(
        UploadAlreadyLockedException.class, () -> service.deleteUpload("/files/test", "owner"));

    assertTrue(Duration.ofNanos(System.nanoTime() - start).toMillis() >= 500);
  }

  @Test
  public void testUploadLockWaitInterrupted() throws Exception {
    UploadLockingService lockingService = mock(UploadLockingService.class);
    when(lockingService.lockUploadByUri("/files/test"))
        .thenThrow(new UploadAlreadyLockedException("Locked"));
    TusFileUploadService service =
        new TusFileUploadService()
            .withUploadStorageService(mock(UploadStorageService.class))
            .withUploadLockingService(lockingService)
            .withUploadLockWaitTimeout(Duration.ofSeconds(5));

    Thread.currentThread().interrupt();
    try {
      assertThrows(IOException.class, () -> service.getUploadInfo("/files/test", "owner"));
    } finally {
      // Clears the flag so it does not leak into other tests
      assertTrue(Thread.interrupted());
    }
  }

  @Test
  public void testNegativeUploadLockWaitTimeoutIsRejected() {
    TusFileUploadService service = new TusFileUploadService();

    assertThrows(
        IllegalArgumentException.class,
        () -> service.withUploadLockWaitTimeout(Duration.ofMillis(-1)));
  }
}
