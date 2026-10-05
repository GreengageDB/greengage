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

1. The `README.Ubuntu.bash` script applies the system configuration and user
   limits during dependency installation. Start a new login session before
   building so the process limits are applied.
