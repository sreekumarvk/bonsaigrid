// Dropped into go-ycsb's cmd/go-ycsb/ at build time so the memcache driver is
// compiled in and self-registers (blank import triggers its init()).
package main

import _ "github.com/pingcap/go-ycsb/db/memcache"
