# Platforms

## For CentOS 7:

- Install dependencies using README.CentOS.bash script:
  ```bash
  ./README.CentOS.bash
  ```
  Note: CentOS 7 is EOL — configure `yum` to use a valid repo (e.g., `vault.centos.org`) before installing dependencies.

## For Rocky Linux (8 or 9)

- Install dependencies using `README.rockylinux.bash`:

  ```bash
  sudo ./README.rockylinux.bash
  ```

- For Rocky Linux 8, create a symbolic link to Python 2 in `/usr/bin`:

  ```bash
  sudo ln -sf /usr/bin/python2 /usr/bin/python
  ```

  > **Note:** Supported Python versions: 2.7 (Rocky 8) or 3.9+ (Rocky 9),
  > selected by the `python` command. For Rocky 9, Python 3 is already
  > configured. For Rocky 8, Python 2 is recommended.

## For Ubuntu (22.04 or 24.04)

- Install dependencies using `README.ubuntu.bash`:

  ```bash
  sudo -E PIP_BREAK_SYSTEM_PACKAGES=1 ./README.ubuntu.bash
  ```

  > **Note:** Ubuntu 24.04 restricts system pip installs.
  > `PIP_BREAK_SYSTEM_PACKAGES` is required to allow this.

- For Ubuntu 22.04, create a symbolic link to Python 2 in `/usr/bin`:

  ```bash
  sudo ln -s python2 /usr/bin/python
  ```

  > **Note:** Supported Python versions: 2.7 or 3.9–3.12, selected by the
  > `python` command. For Ubuntu 24.04, Python 3 is already configured in
  > `README.ubuntu.bash`. For Ubuntu 22.04, Python 2 is recommended.

- Set up the `en_US.UTF-8` locale required to run GPDB:

  ```bash
  sudo locale-gen en_US.UTF-8
  ```

- Optionally, install Kerberos for secure GPDB access:

  ```bash
  sudo apt-get install -y krb5-kdc krb5-admin-server
  ```

  > **Note:** You will be prompted to configure a Kerberos realm. Any value
  > works for local testing. To skip interactive prompts, set
  > `export DEBIAN_FRONTEND=noninteractive` beforehand.

## Common Platform Tasks

1. Set up SSH keys to allow passwordless `ssh localhost`:

   ```bash
   ssh-keygen
   cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
   chmod 600 ~/.ssh/authorized_keys
   ```

2. Verify passwordless SSH to the machine hostname:

   ```bash
   ssh `hostname`
   ```

3. Set up system kernel parameters:

   ```bash
   sudo bash -c 'cat >> /etc/sysctl.conf <<-EOF
   kernel.shmmax = 500000000
   kernel.shmmni = 4096
   kernel.shmall = 4000000000
   kernel.sem = 500 1024000 200 4096
   kernel.sysrq = 1
   kernel.core_uses_pid = 1
   kernel.msgmnb = 65536
   kernel.msgmax = 65536
   kernel.msgmni = 2048
   net.ipv4.tcp_syncookies = 1
   net.ipv4.ip_forward = 0
   net.ipv4.conf.default.accept_source_route = 0
   net.ipv4.tcp_tw_recycle = 1
   net.ipv4.tcp_max_syn_backlog = 4096
   net.ipv4.conf.all.arp_filter = 1
   net.ipv4.ip_local_port_range = 1025 65535
   net.core.netdev_max_backlog = 10000
   net.core.rmem_max = 2097152
   net.core.wmem_max = 2097152
   vm.overcommit_memory = 2
   EOF'
   sudo sysctl -p
   ```

4. Set user and system file descriptor/process limits:

   ```bash
   sudo bash -c 'cat >> /etc/security/limits.conf <<-EOF
   * soft nofile 65536
   * hard nofile 65536
   * soft nproc 131072
   * hard nproc 131072
   EOF'
   su - $USER  # Re-login to apply limits
   ```
