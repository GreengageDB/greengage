# Greengage Database Packaging System

## Overview

This documentation describes the Debian packaging system for Greengage Database (ggdb) located in the `gpAux/` subdirectory. The system builds Debian packages using a custom Makefile and debian/rules file.

## Location and Structure

The packaging system is located in:

```text
./gpAux/
```

The main components are:

- `Makefile` - Contains packaging targets and version management
- `debian/rules` - Debian build rules with custom overrides
- `debian/control` - Package metadata and dependencies
- Other standard Debian packaging files

## Key Components

### Makefile Targets

1. **Version Management**:
   - `../VERSION`: Generates version file using ../getversion
   - `version-vars`: Parses version information and sets build variables
   - `version-info`: Displays version information for debugging

2. **Packaging Targets**:
   - `pkg`: Default target (aliases to pkg-deb)
   - `pkg-deb`: Builds Debian package with default paths
   - `debian/install`: Creates installation manifest
   - `debian/changelog`: Generates Debian changelog automatically
   - `debian/docs`: Generate docfiles list file automatically

### Debian Rules File

The `debian/rules` file uses debhelper (dh) with custom overrides:

1. **Distribution-specific Dependencies**:
   - Automatically detects Ubuntu 22.04 and adds python2.7 dependency

2. **Build Process Overrides**:
   - Skips standard configure and build steps
   - Uses the project's custom `make dist` target for installation
   - Unsets standard compiler flags to avoid conflicts
   - Uses parallel builds with available CPU cores

3. **Control File Generation**:
   - Adds Python dependencies specific to Ubuntu 22.04

## Usage

### Building the Package

From the project root directory, run:

```bash
make -C ./gpAux pkg-deb
```

### Custom Installation Paths

To customize installation paths, set environment variables:

```bash
make -C ./gpAux pkg-deb GGROOT=/custom/path GPDIR=custom_dir
```

### Environment Variables

- `GGROOT`: Installation root directory (default: /opt/greengage)
- `GPDIR` : Subdirectory under `GGROOT` (default: `<Package>` from `debian/control`)

## Build Process Details

1. **Version Generation**:
   - The version is generated using `../getversion`
   - Version string is processed to create package-friendly formats

2. **Package Building**:
   - Uses `debuild` with preserved environment variables
   - Maintains `GGROOT` and `GPDIR` throughout the build process
   - Skips signing (`-us` `-uc` flags)

3. **Installation**:
   - Uses the project's `make dist` target for installation
   - Installs to `debian/tmp` directory for package creation
   - Creates proper file manifest in `debian/install`

## Dependencies

The packaging system automatically handles:

- `Ubuntu 22.04` detection and `python2.7` dependency
- Other distributions may require manual dependency configuration

## Maintenance

### Updating Package Metadata

Edit `debian/control` for:

- Package description
- Maintainer information
- General dependencies

### Adding Distribution Support

Extend the distribution detection in `debian/rules`:

```makefile
ifeq ($(LSB_SI),Ubuntu)
    ifeq ($(LSB_SR),22.04)
        DEPS=python2.7
    endif
    ifeq ($(LSB_SR),20.04)
        DEPS=python2.7
    endif
endif
```

## Notes

- The build skips tests (`DEB_BUILD_OPTIONS=nocheck`) for faster packaging
- Compiler flags are unset to avoid conflicts with the project's build system
- Parallel builds are enabled using all available CPU cores
- The package is built without signing for development convenience
