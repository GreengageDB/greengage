# Greengage Database Packaging System

## Overview

This documentation describes the packaging system for Greengage Database
located in the `gpAux/` subdirectory: Debian packages (`pkg-deb`, using
`debian/rules`) and RPM packages (`pkg-rpm`, using `rpm/greengage7.spec`).

## Location and Structure

The packaging system is located in [gpAux/](../gpAux/):

- [Makefile](Makefile) -
  Defines packaging targets, version management, and artifact collection
- [debian/rules](debian/rules) -
  Debian build rules with custom overrides
- [debian/compat](debian/compat) -
  Debhelper compatibility level
- [debian/control](debian/control) -
  Package metadata and dependencies
- [debian/copyright](debian/copyright) -
  Copyright information
- [debian/lintian-overrides](debian/lintian-overrides) -
  Lintian warning overrides
- [rpm/greengage7.spec](rpm/greengage7.spec) -
  RPM spec file defining package metadata and build steps
- [rpm/greengage7.rpmlintrc](rpm/greengage7.rpmlintrc) -
  `rpmlint` warning overrides, same as `debian/lintian-overrides`

## Usage

### Building the Debian Package

From the project root directory, run:

```bash
make -C ./gpAux pkg-deb
```

### Building the RPM Package

From the project root directory, run:

```bash
make -C ./gpAux pkg-rpm
```

### Custom Installation Paths

To customize installation paths, pass the variables to `make`:

```bash
make -C ./gpAux pkg-deb GPROOT=/custom/path GPDIR=custom_dir
make -C ./gpAux pkg-rpm GPROOT=/custom/path GPDIR=custom_dir
```

### Environment Variables

- `GPROOT`: Installation root directory (default: `/opt/greengagedb`)
- `GPDIR`: Subdirectory under `GPROOT` (default: same as `PACKAGE_NAME`)
- `PACKAGE_NAME`: Package name (default: from `Package:` in `debian/control`,
  e.g., `greengage7`)
- `ARTIFACTS_DIR`: Directory for artifacts (default: `$(CURDIR)/../Package`,
  shared between `.deb` and `.rpm` outputs)
- `RPM_TOPDIR`: `rpmbuild` working directory, used only by `pkg-rpm`
  (default: `$(CURDIR)/../RPM`, kept after the build for inspection)

## Build Process

### Makefile Targets

- `pkg`: Default target (aliases to `pkg-deb`)
- `pkg-deb`: Builds the Debian package
- `pkg-rpm`: Builds the RPM package and checks it with `rpmlint`
- `changelog`: Generates `debian/changelog`
- `debian/install`: Creates the installation manifest
- `../VERSION`: Generates the version file using `../getversion`
- `version-vars`: Sets build variables (`FULL_VERSION`, `PACKAGE_VERSION`,
  `VERSION_CODENAME`, `IS_RELEASE`, `BUILD_TYPE`) from the `../VERSION` file
- `version-info`: Displays version information for debugging

### Version

Both package formats reuse `version-vars`:

- Processes the version string into `FULL_VERSION` and `PACKAGE_VERSION`
- Debian: `IS_RELEASE` and `BUILD_TYPE` are used for the changelog generation
- RPM: `PACKAGE_VERSION` is normalized (`+`/`-` replaced with `.`) and passed
  to `rpmbuild` via `--define gpdb_version`

### Debian Package

`pkg-deb` does the following:

- Executes `debuild` with preserved environment variables (`GPROOT`, `GPDIR`,
  `DESTDIR` as `PACKAGE_NAME`)
- Skips signing with `-us -uc` flags and skips tests
  (`DEB_BUILD_OPTIONS=nocheck`) for faster builds
- Generates `debian/changelog` and the installation manifest `debian/install`
  (both are not committed to the repository)
- Collects only the build artifacts (`.deb`, `.ddeb`, `.build`, `.buildinfo`,
  `.changes`) into `ARTIFACTS_DIR`

The `debian/rules` file uses debhelper (dh) with custom overrides:

- Skips the standard configure, build, clean and optimize debug (`dwz`) steps
- Uses the project's `make dist` target for installation into
  `debian/tmp/$(PACKAGE_NAME)`
- Unsets standard compiler flags to avoid conflicts with the project's build
  system
- Enables parallel builds using all available CPU cores
- Removes the executable bit from Python/Perl/Bash files without shebang and
  from `*.md` files (custom `dh_fixperms`)
- Uses the standard `dh_gencontrol` invocation (no dynamic dependency
  injection)

### RPM Package

`pkg-rpm` does the following:

- Sets up the `rpmbuild` tree under `RPM_TOPDIR`
- Runs `rpmbuild -bb rpm/greengage7.spec` without signing, with `gpdb_version`,
  `gpdb_release`, `gproot`, `gpdir` and `sourcedir` passed via `--define`
- Collects only the resulting `.rpm` into `ARTIFACTS_DIR`
- Runs `rpmlint` with `rpm/greengage7.rpmlintrc` on the collected `.rpm` and
  fails the build if `rpmlint` reports errors (`rpmlint` is installed by
  `README.Rhel-Rocky.bash`)

The `rpm/greengage7.spec` file defines the RPM build:

- `%install` runs `make dist` with `DESTDIR` set to `%{buildroot}` and with
  the standard compiler flags unset, as in `debian/rules`
- Excludes the entire install prefix from `brp-mangle-shebangs` via
  `__brp_mangle_shebangs_exclude_from` — scripts intentionally use
  `#!/usr/bin/env python3` and `#!/usr/bin/env perl`; rewriting shebangs
  would diverge package contents from the source and mask issues
- Strips symbols and debug info from ELF files (executables and `*.so*`)
  via `strip --strip-unneeded`, which also removes BUILDROOT paths embedded by
  the compiler (e.g., PyGreSQL's `_pg.so`) that would otherwise fail
  `check-buildroot` validation
- Removes the executable bit from the same files as the custom `dh_fixperms`
  of `debian/rules`
- Has one `%changelog` entry, its date is set at build time

## Dependencies

### Build Dependencies

Both `pkg-deb` and `pkg-rpm` assume that all build dependencies and the
GPDB build itself (`make dist`) are already satisfied/buildable on the
host — neither target builds GPDB from scratch; they package an already
configured and built source tree.

To get a complete environment with all dependencies installed, use the
provided Docker images instead of installing dependencies manually:

- [ci/Dockerfile.ubuntu](../ci/Dockerfile.ubuntu) - Ubuntu 22.04
- [ci/Dockerfile.rockylinux](../ci/Dockerfile.rockylinux) - Rocky Linux 9

See [ci/readme.md](../ci/readme.md) for instructions on building and
using these images. To install dependencies directly on a host without
Docker, see [README.Linux.md](../README.Linux.md) together with
[README.Ubuntu.bash](README.Ubuntu.bash) (Ubuntu) or
[README.Rhel-Rocky.bash](README.Rhel-Rocky.bash) (RHEL/Rocky Linux).

### Package Dependencies

Package dependencies and conflicts for the Debian package are defined in
`debian/control`.

Package dependencies for the RPM package are defined directly in
`rpm/greengage7.spec` via `Requires:`/`Conflicts:` tags: system utilities
(e.g., `openssh-server`, `rsync`, `zip`), `python3` and `python3-pip`.

The RPM package also requires the library `libxerces-c-3.2.so` (package
`xerces-c`), which comes from EPEL on Rocky Linux 9. Enable EPEL before
installing the package:

```bash
dnf -y install epel-release
```

## Maintenance

### Updating Package Metadata

Edit to update:

| Metadata | DEB (`debian/control`) | RPM (`rpm/greengage7.spec`) |
| --- | --- | --- |
| Package description | `Description` | `Summary`, `%description` |
| Maintainer information | `Maintainer` | — |
| Dependencies | `Depends` | `Requires` |
| Group | `Section` | `Group` |
| License | `debian/copyright` (not in `control`) | `License` |
| URL | — | `URL` |
| Conflicts | `Conflicts` | `Conflicts` |
