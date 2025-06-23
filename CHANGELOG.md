# Changelog

This record is incomplete. The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.10/). I use [Semantic Version](https://semver.rg/spec/v2.0.0.html) to the best of my ability and specifically reject [ZeroVer](https://0ver.org/).

## 1.6.0 (2025-03-22)

### Added
- Allow chained `::` operators and raw pointer types in parsing.
- Add an option to keep a better progress bar during sorting steps.

### Changed
- Improve performance when doing filling steps by filling in an empty dataframe instead of copying the existing dataframe.
- Run more checks even in release for bounds-checking row/column access.

## 1.5.0 (2025-02-23)

### Added
- Allow static methods
- Allow braced initializers in fields
- Allow references and const-qualified types
- Allow more standard headers (`<algoritm>`, `<cstring>`, `<string>`, `<string.h>`)

### Changed
- Synchronization: If bytes are malformed while reading, skip 1 byte ahead at a time until a valid determinant is found

### Fixed
- Account for trailing padding in structs
- Default sort once again sorts by the second real column

## 1.4.0 (2025-02-01)

### Added
- Launch files whose checksum is the sentinel value is `0xDEADBEEF` (little-endian) use format information stored in the file
itself instead of requiring a local copy of the log format.

### Fixed
- Progress bar didn't reset after ever step

## 1.3.0 (2024-08-26)

### Added
- Can load multiple files at the same time
- Add virtual columns for row index and file number

### Changed
- Updated egui to 0.29

### Fixed
- Nullable integer encoding was incorrect 