## For RHEL/Rocky 9:

- Install dependencies using README.Rhel-Rocky.bash script:

  ```bash
  sudo ./README.Rhel-Rocky.bash
  ```

## For Ubuntu 22.04:

- Install Dependencies
  When you run the README.Ubuntu.bash script for dependencies, you will be asked to configure realm for kerberos.
  You can enter any realm, since this is just for testing, and during testing, it will reconfigure a local server/client.
  If you want to skip this manual configuration, use:
  `export DEBIAN_FRONTEND=noninteractive`

  ```bash
  sudo ./README.Ubuntu.bash
  ```

- Set up the kernel parameters

  ```bash
  sudo tee /etc/sysctl.d/90-greengage.conf << EOF
  kernel.shmmax = 5000000000000
  kernel.shmmni = 32768
  kernel.shmall = 40000000000
  kernel.sem = 1000 32768000 1000 32768
  kernel.msgmnb = 1048576
  kernel.msgmax = 1048576
  kernel.msgmni = 32768
  net.core.netdev_max_backlog = 80000
  net.core.rmem_default = 2097152
  net.core.rmem_max = 16777216
  net.core.wmem_max = 16777216
  vm.overcommit_memory = 2
  vm.overcommit_ratio = 95
  EOF
  sudo sysctl -p /etc/sysctl.d/90-greengage.conf
  ```

- Set up the user limits

  ```bash
  sudo tee /etc/security/limits.d/90-greengage.conf << EOF
  * soft nofile 1048576
  * hard nofile 1048576
  * soft nproc 1048576
  * hard nproc 1048576
  EOF
  ```

  The limits are applied to new login sessions. Log in again before you
  create the demo cluster.

## Common Platform Tasks:

Make sure that you add `/usr/local/lib` to `/etc/ld.so.conf`,
then run command `ldconfig`.
1. Create gpadmin and setup ssh keys
   Either use:

   ```bash
   # Requires gpdb clone to be named gpdb_src
   gpdb_src/concourse/scripts/setup_gpadmin_user.bash
   ```
   to create the gpadmin user and set up keys,

   OR

   manually create ssh keys so you can do ssh localhost without a password, e.g., 
   
   ```bash
   ssh-keygen
   cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
   chmod 600 ~/.ssh/authorized_keys
   ```

1. Verify that you can ssh to your machine name without a password.

   ```bash
   ssh <hostname of your machine>  # e.g., ssh briarwood (You can use `hostname` to get the hostname of your machine.)
   ```
