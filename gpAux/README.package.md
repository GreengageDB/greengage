# Greengage Database Packaging System

## Overview

This documentation describes the packaging system for Greengage Database
located in the `gpAux/` subdirectory: Debian packages (`pkg-deb`, using
`debian/rules`) and RPM packages (`pkg-rpm`, using `rpm/greengage7.spec`).

## Location and Structure

The packaging system is located in:

```text
./gpAux/
```

The main components are:

- `Makefile` - Defines packaging targets, version management, artifacts collect
- `debian/rules` - Debian build rules with custom overrides
- `debian/compat` - Debhelper compatibility level
- `debian/control` - Package metadata and dependencies
- `debian/copyright` - Copyright information
- `debian/lintian-overrides` - Lintian warning overrides
- `rpm/greengage7.spec` - RPM spec file defining package metadata and build steps
- `rpm/greengage7.rpmlintrc` - rpmlint warning overrides, analogous to
  `debian/lintian-overrides`

## Key Components

### Makefile Targets

1. **Version Management**:
   - `../VERSION`: Generates version file using `../getversion`
   - `version-vars`: Sets build variables (`FULL_VERSION`, `PACKAGE_VERSION`,
     `VERSION_CODENAME`, `IS_RELEASE`, `BUILD_TYPE`) from `../VERSION` file
   - `version-info`: Displays version information for debugging

2. **Packaging Targets**:
   - `pkg`: Default target (aliases to `pkg-deb`)
   - `pkg-deb`: Builds Debian package, preserves environment variables, and
     collects artifacts (`.deb`, `.ddeb`, `.build`, `.buildinfo`, `.changes`)
   - `pkg-rpm`: Builds RPM package via `rpmbuild` and collects the
     resulting `.rpm` artifact
   - `changelog`: Generates `debian/changelog` (not stored in repo)
   - `debian/install`: Creates installation manifest (not stored in repo)

### Debian Rules File

The `debian/rules` file uses debhelper (dh) with custom overrides:

1. **Build Process Overrides**:
   - Skips standard configure, build, clean and optimize debug (`dwz`) steps
   - Uses the project's `make dist` target for installation
   - Unsets standard compiler flags to avoid conflicts
   - Enables parallel builds using all available CPU cores

2. **Fix Perms Overrides**:
   - Remove executable bit from Python/Perl/Bash modules without shebang

3. **Control File Generation**:
   - Standard `dh_gencontrol` invocation (no dynamic dependency injection)

### RPM Spec File

The `rpm/greengage7.spec` file defines the RPM build:

1. **Dependencies**:
   - `python3` and `python3-pip` required

2. **Build Process**:
   - `%install` invokes the project's `make dist` target with `DESTDIR`
     set to `%{buildroot}`
   - Excludes the entire install prefix from `brp-mangle-shebangs` via
     `__brp_mangle_shebangs_exclude_from` — scripts intentionally use
     `#!/usr/bin/env python` and `#!/usr/bin/env perl`; rewriting shebangs
     would diverge package contents from the source and mask issues
   - Strips debug info from `*.so` files via `strip --strip-debug` to
     remove BUILDROOT paths embedded by the compiler (e.g., PyGreSQL's
     `_pg.so`), which would otherwise fail `check-buildroot` validation
   - Removes the executable bit from Python/Perl/Bash files without a
     shebang and from `*.md` files, mirroring `debian/rules` `dh_fixperms`

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

To customize installation paths, set environment variables:

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
- `RPM_TOPDIR`: `rpmbuild` working directory used only by `pkg-rpm`
  default: `$(CURDIR)/../RPM`, kept after the build for inspection

## Build Process Details

### Debian Package

1. **Version Generation**:
   - Runs `../getversion` to create `../VERSION`
   - Processes version string into `FULL_VERSION` and `PACKAGE_VERSION`
   - Sets `IS_RELEASE` and `BUILD_TYPE` for changelog generation

2. **Package Building**:
   - Executes `debuild` with preserved environment variables (`GPROOT`, `GPDIR`,
     `DESTDIR` as `PACKAGE_NAME`)
   - Skips signing with `-us -uc` flags
   - Collects build artifacts (`.deb`, `.ddeb`, `.build`, `.buildinfo`,
     `.changes`) into `ARTIFACTS_DIR`

3. **Installation**:
   - Uses `make dist` for installation into `debian/tmp/$(PACKAGE_NAME)`
   - Generates file manifest in `debian/install`

### RPM Package

1. **Version Generation**:
   - Reuses `version-vars` (same as Debian) to derive `PACKAGE_VERSION`,
     normalized for RPM (`+`/`-` replaced with `.`) and passed to
     `rpmbuild` via `--define gpdb_version`

2. **Package Building**:
   - Sets up `rpmbuild` tree under `RPM_TOPDIR`
   - Runs `rpmbuild -bb rpm/greengage7.spec` with `gproot`, `gpdir`, and
     `sourcedir` passed via `--define`
   - Collects the resulting `.rpm` into `ARTIFACTS_DIR`

3. **Installation**:
   - Uses `make dist` inside `%install`, installing into `%{buildroot}`

## Build Dependencies

Both `pkg-deb` and `pkg-rpm` assume that all build dependencies and the
GPDB build itself (`make dist`) are already satisfied/buildable on the
host — neither target builds GPDB from scratch; they package an already
configured and built source tree.

To get a complete environment with all dependencies installed, use the
provided Docker images instead of installing dependencies manually:

- `ci/Dockerfile.ubuntu` - Ubuntu 22.04
- `ci/Dockerfile.rockylinux` - Rocky Linux 8/9

See [ci/readme.md](../ci/readme.md) for instructions on building and
using these images. To install dependencies directly on a host without
Docker, see `README.Linux.md` together with `README.Ubuntu.bash` (Ubuntu)
or `README.Rhel-Rocky.bash` (RHEL/Rocky Linux).

## Dependencies

Package dependencies and conflicts for the Debian package are defined in
`debian/control`.

Package dependencies for the RPM package are defined directly in
`rpm/greengage7.spec` via `Requires:`/`Conflicts:` tags.

## Maintenance

### Updating Package Metadata

Edit `debian/control` to update:

- Package description
- Maintainer information
- General dependencies

Edit `rpm/greengage7.spec` to update the equivalent metadata for the
RPM package (`Summary`, `License`, `URL`, `Requires`, `Conflicts`,
`%description`).

## Notes

### Debian

- Skips tests (`DEB_BUILD_OPTIONS=nocheck`) for faster builds
- Unsets compiler flags to avoid conflicts with the project's build system
- Enables parallel builds using all available CPU cores
- Builds without signing for development convenience
- Collects only specific build artifacts into `$(CURDIR)/../Package`:
  (`.deb`, `.ddeb`, `.build`, `.buildinfo`, `.changes`)
- Skips `dh_dwz` step
- Custom `dh_fixperms` removes executable bit from Python/Perl/Bash files
  without shebang and `*.md` files
- Generated `debian/{changelog,install}` is not committed to the repository

### RPM

- Builds without signing for development convenience
- Strips debug info from `*.so` files to remove BUILDROOT paths embedded
  by the compiler, which would otherwise fail `check-buildroot` validation
- Excludes the entire install prefix from shebang mangling via
  `__brp_mangle_shebangs_exclude_from`
- Collects only the resulting `.rpm` into `$(CURDIR)/../Package`
- `RPM_TOPDIR` is kept after the build for inspection
