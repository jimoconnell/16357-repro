# Please note:
This is not a project that would be of interest to the public at large.  It is a simple reproducer created to test and illustrate a very specific behavior.

It uses Hazelcast Enterprise and requires a valid license key to be set as an environment variable. It uses features that are not available in the Community edition of Hazelcast.

If you have a license key, you can set it with `export HZ_LICENSEKEY="ABCD_123_XYZZY... "` (replace with your actual license key).

When that is set, run the script `run-tests.sh`.

This will start a single-member cluster and run the tests in the `TwoMapLockEpClientReproducer` class, print its report, then quit.
