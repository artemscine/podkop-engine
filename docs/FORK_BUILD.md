# Fork build policy

`versions.env` remains identical to the author baseline. Version numbers, package
revisions, source hashes and toolchain pins come from that file without a local
revision suffix or offset. Update it by merging upstream.

`fork.env` selects the default build line (currently 1.13). SDK builds and release
scripts source it after the author metadata; explicit line arguments still work.
Architecture limits remain Cortex A53 and x86_64 in the matrix and SDK build.

The fork adds optional recovery confirmation, exposed by the
`urltest.fallback_recovery` capability. An equal version string does not prove
that two binaries contain equal patches: identify the delivered package by its
SHA-256 and verify the capability. Package managers do not automatically upgrade
an installed package to a different build with the same version.
