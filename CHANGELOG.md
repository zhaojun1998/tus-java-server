# Changelog

All notable changes to this project will be documented in this file.

## [1.0.0-3.3-zfile.3]

### Fixes
- Upload metadata deserialization uses an explicit class allow-list and rejects disallowed nested types before their deserialization callbacks run (backport of upstream #148).
- PATCH and creation-with-upload POST requests accept Content-Type parameters, surrounding whitespace and case variations while still rejecting a different media type (backport of upstream #126).
- Upload locks work on Windows by locking a byte range outside the ownership token. Locks remain mutually exclusive with earlier versions and preserve protection against lock-file replacement.
- Lock ownership checks report filesystem failures instead of treating them as upload contention.

## [1.0.0-3.3-zfile.2]

### Changed
- Appended bytes are collected into 256 KiB blocks before each write to the data file instead of being copied by `FileChannel.transferFrom`, which wrote every few kilobytes received from the servlet container. A single large upload is no longer limited by one CPU core.

## [1.0.0-3.3-zfile.1]

### Added
- **Optional Data Sync**: `withUploadDataSync(boolean)` controls whether appended bytes are forced to the storage device after each request (default: `true`).
- **Expiration of Completed Uploads**: `withCompletedUploadExpiration(boolean)` lets `cleanup()` also remove completed uploads whose expiration timestamp has passed (default: `false`).
- **Lock Wait for Application Calls**: `withUploadLockWaitTimeout(Duration)` lets `getUploadInfo`, `getUploadedBytes`, `getUploadedFile` and `deleteUpload` wait for a locked upload instead of failing immediately (default: zero).
- **Local File of a Completed Upload**: `getUploadedFile(String, String)` returns the data file of a completed upload so it can be moved instead of copied.
- `UploadLockingService#lockUpload(UploadId)` default method to lock an upload by its ID, implemented by `DiskLockingService`.

### Changed
- The offset read from disk is aligned with the size of the data file, so an upload whose bytes or upload information were lost resumes from the bytes actually stored.
- `cleanup()` deletes expired uploads while holding their upload lock and removes upload directories without readable upload information once nothing in them was modified during the upload expiration period.
- Requests that cannot lock their upload are logged as warnings without a stack trace.
- Dependencies use fixed versions instead of version ranges.

### Fixes
- Upload information files are replaced atomically, so a concurrent reader can no longer leave them empty.
- File based locks stay exclusive when the lock file is deleted while another request opens it.

## [1.0.0-3.3]

### Added
- **Creation-with-Upload Extension**: Implemented the optional `creation-with-upload` extension, allowing clients to combine creation and initial file data upload in a single `POST` request.
- **CORS Extension**: Implemented native, out-of-the-box CORS support as an unofficial extension (`cors`) enabled by default. For backward compatibility, it can be disabled via `disableTusExtension("cors")`.

### Changed
- **Stricter Protocol Validation**:
  - Prevent modifying `Upload-Length` headers in subsequent `PATCH` requests.
  - Enforced format and Base64 validations for `Upload-Metadata` headers in `POST` requests.
  - Enforced that `Upload-Defer-Length` header values must be strictly `"1"`.
  - Reject malformed or invalid `Upload-Checksum` headers instead of silently ignoring them.
  - Enabled checksum verification on `POST` requests when using the `creation-with-upload` extension.

### Fixes
  - Only unfinished uploads can expire.
  - Fix for deduplication feature when base64-encoded checksum contains a slash.

## [1.0.0-3.2]

### Added
- **Lock Contention Resolution**: Allow resuming clients to immediately release upload locks held by stalled upload requests via `HEAD` requests. Supports both single-instance and multi-replica/Kubernetes deployments without breaking backward compatibility of the locking interfaces.
- **File Deduplication by Hash**: Implemented optional, space-saving duplicate file detection and linking based on file checksums.
  - Added `withUploadDeduplication(boolean)` builder method on `TusFileUploadService` (default: `false` for backward compatibility).
  - Introduced index system under `<storagePath>/checksums/<algorithm>/<checksum_value>` for mapping file checksums to their original completed upload IDs.
  - Implemented safe read-only recursion in `DiskStorageService` for child uploads: read operations (`getUploadedBytes`, `copyUploadTo`) recursively resolve to the parent upload, while write/truncate operations (`append`, `removeLastNumberOfBytes`) remain strictly bounded to the child ID to avoid accidental parent modifications.
  - Added parent-child expiration coordination: parent upload's expiration timestamp is automatically updated to be greater than or equal to any linked child upload's expiration.
  - Self-cleaning index system: dangling index entries resulting from parent deletion/expiration are automatically detected and removed on the fly.
  - Added new `duplicatesUploadId`, `checksum`, and `checksumAlgorithm` fields to `UploadInfo`.
- **Backward Compatibility**: Explicitly declared `serialVersionUID = -8751200491586638308L` inside `UploadInfo` to prevent serialization version mismatches for pre-existing upload data on disk.
- **Deduplication of Parsing Logic**: Introduced `Utils.ChecksumInfo` and `Utils.parseUploadChecksumHeader` to completely centralize header validation and parsing.
