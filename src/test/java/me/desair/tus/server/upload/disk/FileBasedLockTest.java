package me.desair.tus.server.upload.disk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_DEFAULTS;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import me.desair.tus.server.exception.UploadAlreadyLockedException;
import org.junit.BeforeClass;
import org.junit.Test;

public class FileBasedLockTest {

  private static Path storagePath;

  @BeforeClass
  public static void setupDataFolder() throws IOException {
    storagePath = Paths.get("target", "tus", "locks").toAbsolutePath();
    Files.createDirectories(storagePath);
  }

  @Test
  public void testLockRelease() throws UploadAlreadyLockedException, IOException {
    UUID test = UUID.randomUUID();
    FileBasedLock lock =
        new FileBasedLock("/test/upload/" + test.toString(), storagePath.resolve(test.toString()));
    lock.close();
    assertFalse(Files.exists(storagePath.resolve(test.toString())));
  }

  @Test(expected = UploadAlreadyLockedException.class)
  public void testOverlappingLock() throws Exception {
    UUID test = UUID.randomUUID();
    Path path = storagePath.resolve(test.toString());
    try (FileBasedLock lock1 = new FileBasedLock("/test/upload/" + test.toString(), path)) {
      FileBasedLock lock2 = new FileBasedLock("/test/upload/" + test.toString(), path);
      lock2.close();
    }
  }

  @Test(expected = UploadAlreadyLockedException.class)
  public void testAlreadyLocked() throws Exception {
    UUID test1 = UUID.randomUUID();
    Path path1 = storagePath.resolve(test1.toString());
    try (FileBasedLock lock1 = new FileBasedLock("/test/upload/" + test1.toString(), path1)) {
      FileBasedLock lock2 =
          new FileBasedLock("/test/upload/" + test1.toString(), path1) {
            @Override
            protected FileChannel createFileChannel() throws IOException {
              FileChannel channel = createFileChannelMock();
              doReturn(null).when(channel).tryLock(anyLong(), anyLong(), anyBoolean());
              return channel;
            }
          };
      lock2.close();
    }
  }

  @Test
  public void testLockReleaseLockRelease() throws UploadAlreadyLockedException, IOException {
    UUID test = UUID.randomUUID();
    Path path = storagePath.resolve(test.toString());
    FileBasedLock lock = new FileBasedLock("/test/upload/" + test.toString(), path);
    lock.close();
    assertFalse(Files.exists(path));
    lock = new FileBasedLock("/test/upload/" + test.toString(), path);
    lock.close();
    assertFalse(Files.exists(path));
  }

  @Test(expected = IOException.class)
  public void testLockIoException() throws UploadAlreadyLockedException, IOException {
    // Create directory on place where lock file will be
    UUID test = UUID.randomUUID();
    Path path = storagePath.resolve(test.toString());
    try {
      Files.createDirectories(path);
    } catch (IOException e) {
      fail();
    }

    FileBasedLock lock = new FileBasedLock("/test/upload/" + test.toString(), path);
    lock.close();
  }

  @Test
  public void testLockOnFileUnlinkedByPreviousHolderIsNotShared() throws Exception {
    UUID test = UUID.randomUUID();
    String uri = "/test/upload/" + test;
    Path path = storagePath.resolve(test.toString());
    FileBasedLock previous = new FileBasedLock(uri, path);
    AtomicBoolean previousReleased = new AtomicBoolean();

    // Opens the lock file while the previous holder still has it, then lets that holder release
    // (unlink) it before the OS lock is requested, so the OS lock lands on the unlinked file.
    FileBasedLock late =
        new FileBasedLock(uri, path) {
          @Override
          protected FileChannel createFileChannel() throws IOException {
            FileChannel channel = super.createFileChannel();
            if (previousReleased.compareAndSet(false, true)) {
              previous.release();
            }
            return channel;
          }
        };

    try {
      new FileBasedLock(uri, path).close();
      fail("The lock path must stay locked while the late holder owns the lock");
    } catch (UploadAlreadyLockedException expected) {
      // The late holder retried on the lock file that is linked at the lock path
    } finally {
      late.close();
    }
    assertFalse(Files.exists(path));
  }

  @Test
  public void testReleaseKeepsLockFileReplacedByAnotherHolder() throws Exception {
    UUID test = UUID.randomUUID();
    Path path = storagePath.resolve(test.toString());
    FileBasedLock lock = new FileBasedLock("/test/upload/" + test, path);
    Files.delete(path);
    Files.writeString(path, "other holder");

    lock.close();

    assertEquals("other holder", Files.readString(path));
    Files.delete(path);
  }

  @Test
  public void testChannelIsClosedWhenLockingFails() throws Exception {
    UUID test = UUID.randomUUID();
    AtomicReference<FileChannel> channel = new AtomicReference<>();
    try {
      new FileBasedLock("/test/upload/" + test, storagePath.resolve(test.toString())) {
        @Override
        protected FileChannel createFileChannel() throws IOException {
          FileChannel spy = createFileChannelMock();
          doThrow(new IOException("lock failed"))
              .when(spy)
              .tryLock(anyLong(), anyLong(), anyBoolean());
          channel.set(spy);
          return spy;
        }
      };
      fail("Locking must fail");
    } catch (IOException expected) {
      assertFalse(channel.get().isOpen());
    }
  }

  @Test
  public void testChannelIsClosedWhenWritingTheLockFileFails() throws Exception {
    UUID test = UUID.randomUUID();
    AtomicReference<FileChannel> channel = new AtomicReference<>();
    try {
      new FileBasedLock("/test/upload/" + test, storagePath.resolve(test.toString())) {
        @Override
        protected FileChannel createFileChannel() throws IOException {
          FileChannel spy = createFileChannelMock();
          doReturn(mock(FileLock.class)).when(spy).tryLock(anyLong(), anyLong(), anyBoolean());
          doThrow(new IOException("write failed")).when(spy).truncate(anyLong());
          channel.set(spy);
          return spy;
        }
      };
      fail("Locking must fail");
    } catch (IOException expected) {
      assertFalse(channel.get().isOpen());
    }
  }

  @Test(expected = UploadAlreadyLockedException.class)
  public void testLockFailsWhenTheLockPathIsNeverOwned() throws Exception {
    UUID test = UUID.randomUUID();
    Path path = storagePath.resolve(test.toString());

    // Every opened lock file is unlinked before it is locked, as if other holders kept releasing it
    new FileBasedLock("/test/upload/" + test, path) {
      @Override
      protected FileChannel createFileChannel() throws IOException {
        FileChannel channel = super.createFileChannel();
        Files.delete(path);
        return channel;
      }
    };
  }

  @Test(expected = UploadAlreadyLockedException.class)
  public void testFailingCloseDoesNotHideTheLockResult() throws Exception {
    UUID test = UUID.randomUUID();
    new FileBasedLock("/test/upload/" + test, storagePath.resolve(test.toString())) {
      @Override
      protected FileChannel createFileChannel() {
        // tryLock returns null as when another process holds the lock, and closing fails
        return mock(
            FileChannel.class,
            withSettings()
                .useConstructor()
                .defaultAnswer(
                    invocation -> {
                      if ("implCloseChannel".equals(invocation.getMethod().getName())) {
                        throw new IOException("close failed");
                      }
                      return RETURNS_DEFAULTS.answer(invocation);
                    }));
      }
    };
  }

  private FileChannel createFileChannelMock() throws IOException {
    return spy(FileChannel.class);
  }
}
