## For CentOS 7:

- Install dependencies using README.CentOS.bash script:
  ```bash
  ./README.CentOS.bash
  ```
  Note: CentOS 7 is EOL — configure `yum` to use a valid repo (e.g., `vault.centos.org`) before installing dependencies.

## For RHEL/Rocky 8:

- Install dependencies using README.Rhel-Rocky.bash script:
  ```bash
  ./README.Rhel-Rocky.bash
  ```

- Build and install zstd with static library, e.g.:
  ```bash
  cd /tmp
  curl -LO https://github.com/facebook/zstd/releases/download/v1.4.4/zstd-1.4.4.tar.gz
  tar -xf zstd-1.4.4.tar.gz
  cd zstd-1.4.4
  make -j$(nproc)
  sudo make install PREFIX=/usr/local
  ```

- Create symbolic link to Python 2 in `/usr/bin`:

  ```bash
  sudo ln -s python2 /usr/bin/python
  ```

## For Ubuntu (versions 22.04 or 24.04):

- Install dependencies using README.ubuntu.bash script:
  ```bash
  sudo ./README.ubuntu.bash
  ```

- For Ubuntu 22.04, create symbolic link to Python 2 in `/usr/bin`:

  ```bash
  sudo ln -s python2 /usr/bin/python
  ```
  Note: Supported Python versions: 2.7 or 3.9 to 3.12. The version is selected
  by the `python` command. For Ubuntu 24.04, Python3 is already configured
  in `README.ubuntu.bash`. For Ubuntu 22.04, we recommend using Python2.

- Ensure that your system supports American English with an internationally compatible character encoding scheme. To do this, run:
  ```bash
  sudo locale-gen "en_US.UTF-8"
  ```
  
- Optionally, installing Kerberos may be required to configure secure access to GPDB. To install Kerberos, run:
  ```bash
  sudo apt-get install -y krb5-kdc krb5-admin-server
  ```
  Note: You will be asked to configure realm for Kerberos. You can enter any realm, since this is just for testing,
  and during testing, it will reconfigure a local server/client. If you want to skip this manual configuration, use:
  `export DEBIAN_FRONTEND=noninteractive`

## Common Platform Tasks:

1. Setup SSH keys so you can run ssh localhost without a password, e.g., 
   
    ```bash
    ssh-keygen
    cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
    chmod 600 ~/.ssh/authorized_keys
    ```

2. Verify that you can ssh to your machine name without a password

    ```bash
    ssh `hostname`  # e.g., ssh briarwood
    ```

3. Set up your system configuration:

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
    sudo sysctl -p # Apply settings
    ```      

4. Change user and system limits:
    ```bash
    sudo bash -c 'cat >> /etc/security/limits.conf <<-EOF
    * soft nofile 65536
    * hard nofile 65536
    * soft nproc 131072
    * hard nproc 131072
    EOF'
    su - $USER # Apply settings
    ```

