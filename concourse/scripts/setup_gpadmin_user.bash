#!/bin/bash

# Based on install_hawq_toolchain.bash in Pivotal-DataFabric/ci-infrastructure repo

set -uxo pipefail
setup_ssh_for_user() {
  local user="${1}"
  local home_dir
  home_dir=$(eval echo "~${user}")

  mkdir -p "${home_dir}/.ssh"
  touch "${home_dir}/.ssh/authorized_keys" "${home_dir}/.ssh/known_hosts" "${home_dir}/.ssh/config"
  if [ ! -f "${home_dir}/.ssh/id_rsa" ]; then
    ssh-keygen -t rsa -N "" -f "${home_dir}/.ssh/id_rsa"
  fi
  cat "${home_dir}/.ssh/id_rsa.pub" >> "${home_dir}/.ssh/authorized_keys"
  chmod 0600 "${home_dir}/.ssh/authorized_keys"
  cat << 'NOROAMING' >> "${home_dir}/.ssh/config"
Host *
  UseRoaming no
NOROAMING
  chown -R "${user}" "${home_dir}/.ssh"
}

ssh_keyscan_for_user() {
  local user="${1}"
  local home_dir
  home_dir=$(eval echo "~${user}")

  {
    ssh-keyscan localhost
    ssh-keyscan 0.0.0.0
    ssh-keyscan `hostname`
  } >> "${home_dir}/.ssh/known_hosts"
}

chown_if_needed() {
    local dir="$1"
    [ -d "$dir" ] || return 0

    local last_fixed=""
    while IFS= read -r bad_dir; do
        # skip directories already covered by a previously-fixed ancestor
        if [ -n "$last_fixed" ] && [[ "$bad_dir" == "$last_fixed"/* ]]; then
            continue
        fi
        echo "WARNING: wrong ownership on '$bad_dir', running chown -R gpadmin:gpadmin" >&2
        chown -R gpadmin:gpadmin "$bad_dir"
        last_fixed="$bad_dir"
    done < <(find "$dir" -xdev -type d \( ! -user gpadmin -o ! -group gpadmin \))
}


transfer_ownership() {
    # There are dependent projects which are rely on changing ownership of files
    # lets make this optional.

    chmod a+w gpdb_src
    find gpdb_src -type d -exec chmod a+w {} \;
    # Needed for the gpload test
    [ -f gpdb_src/gpMgmt/bin/gpload_test/gpload2/data_file.csv ] && chown gpadmin:gpadmin gpdb_src/gpMgmt/bin/gpload_test/gpload2/data_file.csv

    chown_if_needed /usr/local/gpdb
    chown_if_needed /usr/local/greenplum-db-devel
    chown_if_needed /home/gpadmin
}

set_limits() {
  # Currently same as what's recommended in install guide
  if [ -d /etc/security/limits.d ]; then
    cat > /etc/security/limits.d/gpadmin-limits.conf <<-EOF
		gpadmin soft core unlimited
		gpadmin soft nproc 131072
		gpadmin soft nofile 65536
	EOF
  fi
  # Print now effective limits for gpadmin
  su gpadmin -c 'ulimit -a'
}

add_gpadmin_user() {
  if ! getent group supergroup > /dev/null; then
    groupadd supergroup
  fi

  TEST_OS=$(determine_os)

  case "$TEST_OS" in
    centos*)
      /usr/sbin/useradd -G supergroup,tty gpadmin
      ;;
    ubuntu*)
      /usr/sbin/useradd -G supergroup,tty gpadmin -s /bin/bash
      ;;
    sles*)
      # create a default group gpadmin, and add user gpadmin to group gapdmin, supergroup, tty
      /usr/sbin/useradd -U -G supergroup,tty gpadmin
      ;;
    photon*)
      /usr/sbin/useradd -U -G supergroup,tty,root gpadmin
      ;;
    *) echo "Unknown OS: $TEST_OS"; return 1 ;;
  esac

  echo -e "password\npassword" | passwd gpadmin
  # Add user to sudoers list required for gpinitsystem test
  echo "gpadmin ALL = NOPASSWD : ALL" >> /etc/sudoers
  set_limits
}

setup_gpadmin_user() {
  # Perform actions when the container starts
  setup_ssh_for_user gpadmin
  transfer_ownership
}

setup_sshd() {

  test -e /etc/ssh/ssh_host_rsa_key || ssh-keygen -f /etc/ssh/ssh_host_rsa_key -N '' -t rsa
  test -e /etc/ssh/ssh_host_dsa_key || ssh-keygen -f /etc/ssh/ssh_host_dsa_key -N '' -t dsa
  # See https://gist.github.com/gasi/5691565
  sed -ri 's/UsePAM yes/UsePAM no/g' /etc/ssh/sshd_config
  # Disable password authentication so builds never hang given bad keys
  sed -ri 's/PasswordAuthentication yes/PasswordAuthentication no/g' /etc/ssh/sshd_config

  echo "MaxStartups 100:30:200" >> /etc/ssh/sshd_config

  case "$TEST_OS" in
    centos6 | sles*)
      test -e /etc/ssh/ssh_host_key || ssh-keygen -f /etc/ssh/ssh_host_key -N '' -t rsa1
      ;;
    photon*)
      test -e /etc/ssh/ssh_host_ecdsa_key || ssh-keygen -f /etc/ssh/ssh_host_ecdsa_key -N '' -t ecdsa
      test -e /etc/ssh/ssh_host_ed25519_key || ssh-keygen -f /etc/ssh/ssh_host_ed25519_key -N '' -t ed25519
      ;;
    centos7)
      test -e /etc/ssh/ssh_host_key || ssh-keygen -f /etc/ssh/ssh_host_key -N '' -t rsa1
      # For Centos 7, disable looking for host key types that older Centos versions don't support.
      sed -ri 's@^HostKey /etc/ssh/ssh_host_ecdsa_key$@#&@' /etc/ssh/sshd_config
      sed -ri 's@^HostKey /etc/ssh/ssh_host_ed25519_key$@#&@' /etc/ssh/sshd_config
      ;;
  esac

  setup_ssh_for_user root

  if [[ "$TEST_OS" == *"ubuntu"* ]]; then
    mkdir -p /var/run/sshd
    chmod 0755 /var/run/sshd
  fi

  /usr/sbin/sshd

  ssh_keyscan_for_user root
  ssh_keyscan_for_user gpadmin
}


determine_os() {
  local name version
  if [ -f /etc/redhat-release ]; then
    name="centos"
    version=$(sed </etc/redhat-release 's/.*release *//' | cut -f1 -d.)
  elif [ -f /etc/SuSE-release ]; then
    name="sles"
    version=$(awk -F " *= *" '$1 == "VERSION" { print $2 }' /etc/SuSE-release)
  elif  grep -q photon /etc/os-release  ; then
    name="photon"
    version=$( awk -F " *= *" '$1 == "VERSION_ID" { print $2 }' /etc/os-release | cut -f1 -d.)
  elif grep -q ubuntu /etc/os-release ; then
    name="ubuntu"
    version=$(awk -F " *= *" '$1 == "VERSION_ID" { print $2 }' /etc/os-release | tr -d \")
  else
    echo "Could not determine operating system type" >/dev/stderr
    exit 1
  fi
  echo "${name}${version}"
}

# Set the "Set-User-ID" bit of ping, or else gpinitsystem will error by following message:
# [FATAL]:-Unknown host d6f9f630-65a3-4c98-4c03-401fbe5dd60b: ping: socket: Operation not permitted
# This is needed in centos7, sles12sp5, but not for centos6, ubuntu18.04
workaround_before_concourse_stops_stripping_suid_bits() {
  chmod u+s $(which ping)
}

_main() {
  set -e
  TEST_OS=$(determine_os)
  setup_gpadmin_user
  setup_sshd
  workaround_before_concourse_stops_stripping_suid_bits
}

echo "Starting"
[ "${BASH_SOURCE[0]}" = "$0" ] && _main "$@"
