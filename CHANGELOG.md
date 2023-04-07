# Change Log

## Current Version

v2.0.x

- Major internal refactor and breaking changes
- Consolidated settings, callbacks, and statistics into new objects
- Paginated enumeration including prefix-based search
- Refactored many APIs for simplification
- Internal table structure extended
- TryGet APIs
- Additional properties on ```DedupeObject``` including compressed length and number of chunks

## Previous Versions

v1.5.0

- DedupeStream class to read large, deduplicated objects using a stream

v1.4.1

- XML documentation

v1.4.0

- Support for external database providers (use your own database)
- Internal refactor

v1.3.x

- Better support for large files with APIs for streams
- Moved internal chunking operations to use streams for both byte arrays and streams
- Removed the XL projects

v1.2.x

- Bugfixes and code consolidation
- Configurable callbacks to better support custom/contextual read/write/delete behavior

v1.1.x

- Bugfixes and code consolidation
- ObjectMetadata class to simplify code
- Added chunk exists method

v1.0.x

- Initial release
- Added debugging for both dedupe and SQLite database calls
- Added DedupeLibraryXL project for multiple containers (databases) over a single chunk repository
- Added delete container API
- Added import container index API
- Added store or replace object API
- Added locking mechanisms for correctness
- Added backup APIs
- Retarget to .NET Core and .NET Framework
- Migrated from Mono.Data.Sqlite to System.Data.Sqlite
