# Setting the product name at build time

Distributions that ship Greengage under a different product name have
historically carried a patch set against the sources for that purpose alone.
The naming is a build parameter, not a behaviour change, so `configure` exposes
it directly.

The option defaults to the stock upstream value: a build that does not pass it
is identical to a build of the unmodified sources.

## The option

### `--with-product-name=NAME`

The product, as one word and without the ` Database` that is appended to it.
`--with-product-name=Greenplum` names the product `Greenplum Database`; the
default names it `Greengage Database`.

` Database` is appended rather than taken from the caller because
`gpMgmt/bin/gppylib/gpversion.py` parses `select version()` with the regexp
`Green\w+ Database`, and half of that pattern is then guaranteed. What remains
to check is `NAME` itself, which must match `Green\w+` — letters, digits and
underscores after `Green`, so no spaces, hyphens or dots. `configure` refuses
anything else, including a `NAME` that already carries ` Database`.

The name sets two things.

**The product name**, which determines:

* `PACKAGE_NAME` and `PACKAGE_STRING` in `pg_config.h`, and through them the
  `... Database / ...` GUC categories in `pg_settings.category` and a handful of
  error message texts;
* `PG_VERSION_STR`, and through it the output of `select version()`;
* `GP_PRODUCT_NAME`, used by the `--version` and `--gp-version` output of
  `postgres`, `initdb`, `pg_ctl`, `pg_controldata`, `pg_resetxlog`,
  `pg_rewind`, `pg_restore`, `pg_basebackup` and the `src/bin/scripts`
  utilities.

The same value must be used for the whole installation: `pg_ctl`,
`pg_basebackup` and `pg_rewind` locate their helper binaries with
`find_other_exec()`, which compares the `--version` output verbatim.

**The environment script alias**, a symlink `<name>_path.sh` created next to
`$GPHOME/greengage_path.sh` for installations whose users source the script
under that name. `NAME` is lower cased for it. The real file is always
`greengage_path.sh`; the alias points at it. No symlink is created for a default
build, or for a `NAME` that would name the file after itself.

## Example

```
./configure --with-product-name=Greengrocer
```

names the product `Greengrocer Database` and symlinks
`$GPHOME/greengrocer_path.sh` to `greengage_path.sh`. The same flag can be
passed through `gpAux`:

```
make -C gpAux CONFIGURE_FLAGS='--with-product-name=Greengrocer' dist
```

## Scope

The option covers the version strings only. Product names embedded in
`errmsg()`/`errdetail()` texts, in comments, in documentation and in package
names are unaffected — their values are pinned by hundreds of regression test
expected files.

The MSVC client build does not parameterize the product name: it reads
`src/include/pg_config.h.win32`, which is a static file and does not go through
`configure`.
