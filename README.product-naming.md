# Setting the product name at build time

Distributions that ship Greengage under a different product name have
historically carried a patch set against the sources for that purpose alone.
The naming is a build parameter, not a behaviour change, so `configure` exposes
it directly.

All options below default to the stock upstream values: a build that passes
none of them is identical to a build of the unmodified sources.

## Options

### `--with-product-name=NAME`

Sets the product name used in version strings. Defaults to
`Greengage Database`. It determines:

* `PACKAGE_NAME` and `PACKAGE_STRING` in `pg_config.h`, and through them the
  `Greengage Database / ...` GUC categories in `pg_settings.category` and a
  handful of error message texts;
* `PG_VERSION_STR`, and through it the output of `select version()`;
* `GP_PRODUCT_NAME`, used by the `--version` and `--gp-version` output of
  `postgres`, `initdb`, `pg_ctl`, `pg_controldata`, `pg_resetxlog`,
  `pg_rewind`, `pg_restore`, `pg_basebackup` and the `src/bin/scripts`
  utilities.

The same value must be used for the whole installation: `pg_ctl`,
`pg_basebackup` and `pg_rewind` locate their helper binaries with
`find_other_exec()`, which compares the `--version` output verbatim.

**Constraint.** The name must match `Green\w+ Database` — `\w` being
`[a-zA-Z0-9_]`, so no spaces, hyphens or dots between `Green` and ` Database`.
That is the pattern `gpMgmt/bin/gppylib/gpversion.py` uses to parse
`select version()`. A name outside it makes `GpVersion()` raise on every server
started by the build, which takes down `gpstart`, `gpstop`, `gppkg`, `analyzedb`
and `gpload`. `configure` enforces the pattern and refuses such a name.

### `--with-env-script-alias=NAME`

An extra symlink created next to `$GPHOME/greengage_path.sh`, for installations
whose users source the environment script under a historical name. The real file
is always `greengage_path.sh`; the alias points at it. Empty by default, in which
case no symlink is created.

### `--with-hashable-eq-symbol=SYMBOL`

Exports `is_builtin_greengage_hashable_equality_between_same_type()` under
`SYMBOL` instead of its upstream name. This is an ABI escape hatch for
distributions whose already released extensions resolve the function under a
historical name at `dlopen()` time. Empty by default, in which case the
upstream name is exported.

The value is a C identifier, not a string, and it *renames* the symbol rather
than adding an alias. A build that sets it is therefore **ABI incompatible with
the upstream build** and must be validated against the distribution's own
`.abi-check` baselines rather than the ones in this repository.

## Example

```
./configure \
    --with-product-name="Historic Database" \
    --with-env-script-alias=historic_path.sh \
    --with-hashable-eq-symbol=is_builtin_historic_hashable_equality_between_same_type
```

The same flags can be passed through `gpAux`:

```
make -C gpAux CONFIGURE_FLAGS='--with-product-name="Historic Database" \
    --with-env-script-alias=historic_path.sh' dist
```

## Scope

The options cover the version strings and the exported symbol only. Product
names embedded in `errmsg()`/`errdetail()` texts, in comments, in documentation
and in package names are unaffected — their values are pinned by hundreds of
regression test expected files.

The MSVC client build does not parameterize the product name: it reads
`src/include/pg_config.h.win32`, which is a static file and does not go through
`configure`.
