# Greengage Database Packaging System

## Overview

This documentation describes the Debian packaging system for Greengage Database
located in the `gpAux/` subdirectory. The system builds Debian packages using a
custom Makefile and `debian/rules` file.

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

## Key Components

### Makefile Targets

1. **Version Management**:
   - `../VERSION`: Generates version file using `../getversion`
   - `version-vars`: Sets build variables (`FULL_VERSION`, `PACKAGE_VERSION`,
     `DISTRO_CODENAME`, `IS_RELEASE`, `BUILD_TYPE`) from `../VERSION` file
   - `version-info`: Displays version information for debugging

2. **Packaging Targets**:
   - `pkg`: Default target (aliases to `pkg-deb`)
   - `pkg-deb`: Builds Debian package, preserves environment variables, and
     collects artifacts (`.deb`, `.ddeb`, `.build`, `.buildinfo`, `.changes`)
   - `changelog`: Generates `debian/changelog` (not stored in repo)
   - `debian/install`: Creates installation manifest (not stored in repo)

### Debian Rules File

The `debian/rules` file uses debhelper (dh) with custom overrides:

1. **Distribution-specific Dependencies**:
   - Detects Ubuntu 22.04 and sets `python2`, `python2.7` as dependencies with
     `python-is-python3` conflict
   - Other Ubuntu versions use `python3`
   - Non-Ubuntu distributions same as for Ubuntu 22.04

2. **Build Process Overrides**:
   - Skips standard configure, build, and clean steps
   - Uses the project's `make dist` target for installation
   - Unsets standard compiler flags to avoid conflicts
   - Enables parallel builds using all available CPU cores

3. **Control File Generation**:
   - Injects Python dependencies via `-VpythonRequires`, `-VpythonConflicts`
   options in `dh_gencontrol`

## Usage

### Building the Package

From the project root directory, run:

```bash
make -C ./gpAux pkg-deb
```

### Custom Installation Paths

To customize installation paths, set environment variables:

```bash
make -C ./gpAux pkg-deb GPROOT=/custom/path GPDIR=custom_dir
```

### Environment Variables

- `GPROOT`: Installation root directory (default: `/opt/greengagedb`)
- `GPDIR`: Subdirectory under `GPROOT` (default: same as `PACKAGE_NAME`)
- `PACKAGE_NAME`: Package name (default: from `Package:` in `debian/control`,
   e.g., `greengage6`)
- `ARTIFACTS_DIR`: Directory for artifacts (default: `$(CURDIR)/../Package`)

## Build Process Details

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

## Dependencies

Package dependencies and conflicts are defined in `debian/control`.
Python dependencies and conflicts are dynamically detected in `debian/rules` and
injected via `${pythonRequires}` and `${pythonConflicts}` substitution variables.

## Maintenance

### Updating Package Metadata

Edit `debian/control` to update:

- Package description
- Maintainer information
- General dependencies

### Adding Distribution Support

Modify distribution detection in `debian/rules`:

```makefile
ifeq ($(LSB_SI),Ubuntu)
  ifeq ($(LSB_SR),22.04)
    DEPS = python2,python2.7
    CONFLICTS = python-is-python3
  else
    DEPS = python3
  endif
else
  DEPS = python2,python2.7
  CONFLICTS = python-is-python3
endif
```

## Notes

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
