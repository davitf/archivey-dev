## ADDED Requirements

### Requirement: Independently opened member streams operate independently

Member streams obtained from the same archive SHALL be independent: each stream has
its own position, and reading or seeking one stream SHALL NOT change the position of,
or corrupt the data returned by, any other open stream — including a second stream
for the **same** member. Different threads MAY each use a different member stream
concurrently. When the archive was opened from a caller-provided stream (a single
underlying file position), Archivey SHALL coordinate access internally (e.g. a
locked, seek-per-read multiplexer) rather than letting concurrent member streams
corrupt each other.

This capability is at **exploration stage**: the contract's strength (shareability of
a single stream object across threads, solid-archive cost/restriction trade-offs,
subprocess-backed streams, free-threaded builds) is being decided; the requirement
fixes only the parts already decided.

#### Scenario: Two member streams read independently
- **WHEN** two members of a random-access archive are opened and read with
  interleaved `read()` calls
- **THEN** each stream returns exactly its member's bytes, as if each had been read
  alone

#### Scenario: Same member opened twice
- **WHEN** the same member is opened twice and the two streams are read to different
  positions
- **THEN** the two streams maintain independent positions and both return correct
  data

#### Scenario: Streams used from different threads
- **WHEN** two threads each own a different open member stream and read concurrently
- **THEN** both reads complete correctly without external locking by the caller
