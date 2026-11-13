# Upload metadata compatibility fixture

`upload-info-zfile.2.ser` was written using the published
`com.github.zhaojun1998:tus-java-server:1.0.0-3.3-zfile.2` JAR, not the classes under test.
It contains UUID and numeric upload IDs, concatenation parts, metadata and checksum fields.

To regenerate with that release JAR and its runtime dependencies on `LEGACY_CLASSPATH`:

```sh
java -cp "$LEGACY_CLASSPATH" GenerateLegacyUploadInfo.java upload-info-zfile.2.ser
```
