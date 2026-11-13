import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.UUID;
import me.desair.tus.server.checksum.ChecksumAlgorithm;
import me.desair.tus.server.upload.UploadId;
import me.desair.tus.server.upload.UploadInfo;
import me.desair.tus.server.upload.UploadType;

// Run with the published zfile.2 JAR to retain an independent compatibility fixture.
class GenerateLegacyUploadInfo {
  public static void main(String[] args) throws Exception {
    UploadInfo info = new UploadInfo();
    info.setId(new UploadId(UUID.fromString("12345678-1234-1234-1234-123456789abc")));
    info.setUploadType(UploadType.CONCATENATED);
    info.setLength(12L);
    info.setOffset(4L);
    info.setOwnerKey("legacy-owner");
    info.setEncodedMetadata("filename bGVnYWN5LnR4dA==");
    info.setConcatenationPartIds(new LinkedList<>(Arrays.asList("part-one", "part-two")));
    info.setDuplicatesUploadId(new UploadId(42L));
    info.setChecksumAlgorithm(ChecksumAlgorithm.SHA256);
    info.setChecksum("legacy-checksum");
    try (ObjectOutputStream output = new ObjectOutputStream(Files.newOutputStream(Path.of(args[0])))) {
      output.writeObject(info);
    }
  }
}
