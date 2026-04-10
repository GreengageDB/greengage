# Greengage Packaging System

## Overview

The packaging system produces OS-native packages for Greengage Database.
It reuses the project's `make dist` installation flow — the same target
used for development builds — and wraps it with platform-specific packaging
tools.

The Makefile in `gpAux/` is the single entry point.  For Debian packaging
it invokes `debuild` (from the `devscripts` package), which in turn calls
back into this same Makefile recursively via `debian/rules` to perform the
actual installation into a staging directory.  Standard build targets
(`configure`, `build`, `clean`) are overridden and skipped; only
`make dist` is used.

### Supported formats

| Format | Status | Tooling |
|---|---|---|
| Debian (`.deb`) | Active | debhelper + debuild |
| RPM (`.rpm`) | TODO | — |

## Build Flow (Debian)

```
../getversion → ../VERSION → version-vars → debian/changelog
                                                  ↓
make pkg-deb → debuild → debian/rules → make dist → .deb / .ddeb
```

`debuild` re-invokes this Makefile through `debian/rules` using the
standard debhelper targets (`override_dh_auto_install`, etc.).  The
`override_dh_auto_install` target runs `make dist` with `DESTDIR`
pointing at the staging area.

## Makefile Targets

### Version

| Target | Description |
|---|---|
| `../VERSION` | Runs `../getversion`, writes result to `../VERSION` |
| `version-vars` | Reads `../VERSION`, sets `FULL_VERSION`, `PACKAGE_VERSION`, `DISTRO_CODENAME`, `IS_RELEASE`, `STABILITY`, `BUILD_TYPE` |
| `version-info` | Prints the above variables (debug) |

### Packaging

| Target | Description |
|---|---|
| `pkg` | Alias for `pkg-deb` |
| `pkg-deb` | Main build: generates changelog + install manifest, runs `debuild`, collects artifacts into `../Package/` |
| `changelog` | Generates `debian/changelog` from version-vars |
| `debian/install` | Generates install manifest (`debian/install`) |

## debian/rules

Standard debhelper flow with overrides:

| Override | Behaviour |
|---|---|
| `dh_auto_clean` | Skipped |
| `dh_auto_configure` | Skipped |
| `dh_auto_build` | Skipped |
| `dh_auto_install` | Runs `make dist` with `DESTDIR=debian/tmp/$(PACKAGE_NAME)`, `GPROOT=/opt/greengagedb`, `GPDIR=$(PACKAGE_NAME)`, parallel `-j$(nproc)`. Unsets `CFLAGS`, `CPPFLAGS`, `CXXFLAGS`, `LDFLAGS` |
| `dh_dwz` | Skipped |
| `dh_fixperms` | Strips executable bit from `.py`, `.pm`, `.sh` files without a shebang |
| `dh_gencontrol` | Standard |

## Environment Variables

| Variable | Default | Purpose |
|---|---|---|
| `GPROOT` | `/opt/greengagedb` | Installation root |
| `GPDIR` | value from `debian/control` (e.g. `greengage7`) | Subdirectory under `GPROOT` |
| `PACKAGE_NAME` | value from `debian/control` | Package name |
| `ARTIFACTS_DIR` | `$(CURDIR)/../Package` | Where build artifacts are collected |

## Build Artifacts

`pkg-deb` moves these files from the parent directory into `ARTIFACTS_DIR`:

- `*.deb` — binary package
- `*.ddeb` — debug symbols
- `*.build`, `*.buildinfo`, `*.changes` — build metadata

## Dependencies

Declared in `debian/control`:

```
iproute2, iputils-ping, less, openssh-client, openssh-server,
openssl, python3, python3-pkg-resources, python3-psutil, python3-psycopg2, python3-yaml, rsync, zip, net-tools
```

## Usage

```bash
# Build package
make -C ./gpAux pkg-deb

# Custom install path
make -C ./gpAux pkg-deb GPROOT=/opt/custom GPDIR=custom-db

# Debug version variables
make -C ./gpAux version-info
```

## Key Files

| File | Purpose |
|---|---|
| `Makefile` | Version management, packaging targets, artifact collection |
| `debian/rules` | Debhelper overrides, `make dist` integration |
| `debian/control` | Package metadata, dependencies |
| `debian/lintian-overrides` | Suppressed lintian warnings |
