# libmdbx-rs


Rust bindings for [libmdbx](https://github.com/Mithril-mine/libmdbx).

Forked from an earlier Apache licenced version of the `libmdbx-rs` crate, before it changed licence to GPL.
NOTE: Most of the repo came from [lmdb-rs bindings](https://github.com/mozilla/lmdb-rs).

## Updating the libmdbx Version

Since 0.14.x, libmdbx is distributed in amalgamated source form. Clone and copy the sources directly:

```bash
# clone libmdbx
git clone https://github.com/Mithril-mine/libmdbx.git ../libmdbx

# copy sources into mdbx-sys/libmdbx
rm -rf mdbx-sys/libmdbx
cp -R ../libmdbx mdbx-sys/libmdbx

# add the changes to the next commit you will make
git add mdbx-sys/libmdbx
```
