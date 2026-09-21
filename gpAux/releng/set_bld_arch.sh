#!/bin/sh
# ----------------------------------------------------------------------
# Define ARCH values
# ----------------------------------------------------------------------

case "$(uname -s)" in
    Linux)
    if [ -f /etc/redhat-release ] && [ ! -f /etc/altlinux-release ] && [ ! -f /etc/redos-release ] && [ ! -f /etc/rocky-release ]; then
        BLD_ARCH_HOST="rhel$(cat /etc/redhat-release | sed -e 's/CentOS Linux/RedHat/' -e 's/Red Hat Enterprise Linux/RedHat/' -e 's/WS//' -e 's/Server//' -e 's/Client//' | awk '{print $3}' | awk -F. '{print $1}')_$(uname -m | sed -e s/i686/x86_32/)"
    fi

    if [ -f /etc/altlinux-release ]; then
        BLD_ARCH_HOST="$(. /etc/os-release; echo ${ID}${VERSION_ID} | sed 's/-/_/' | cut -d'.' -f1,2)_$(uname -m)"
    fi
    if [ -f /etc/rocky-release ]; then
        BLD_ARCH_HOST="$(. /etc/os-release; echo ${ID}$(echo ${VERSION_ID} | cut -d. -f1)_$(uname -m))"
    fi

    if [ -f /etc/astra_version ]; then
        BLD_ARCH_HOST="$(. /etc/os-release; echo ${ID}${VERSION_ID} | sed 's/-/_/')"
    fi

    if [ -f /etc/redos-release ]; then
        BLD_ARCH_HOST="$(. /etc/os-release; echo ${ID}${VERSION_ID} | sed 's/-/_/')"
    fi

    if [ -z "${BLD_ARCH_HOST}" ] && [ -f /etc/os-release ]; then
        BLD_ARCH_HOST="$(. /etc/os-release; echo ${ID}${VERSION_ID}_$(uname -m))"
    fi
    ;;
    *)
    BLD_ARCH_HOST="BLD_ARCH_unknown"
    ;;
esac

echo ${BLD_ARCH_HOST}
