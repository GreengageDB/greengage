## credcheck - PostgreSQL username/password checks

- [credcheck - PostgreSQL username/password checks](#credcheck---postgresql-usernamepassword-checks)
	- [Description](#description)
	- [Installation](#installation)
	- [Checks](#checks)
	- [Password reuse policy](#password-reuse-policy)
	- [Examples](#examples)
	- [Limitations](#limitations)
	- [Authors](#authors)
	- [License](#license)
	- [Credits](#credits)


### [Description](#description)

The `credcheck` PostgreSQL extension provides few general credential checks, which will be evaluated during the user creation, during the password change and user renaming. By using this extension, we can define a set of rules:

- allow a specific set of credentials
- reject a certain type of credentials
- enforce use of an expiration date with a minimum of day for a password
- define a password reuse policy

This extension is developed based on the PostgreSQL's `check_password_hook` hook.

This extension provides all the checks as configurable parameters. The default configuration settings, will not enforce any complex checks and will try to allow most of the credentials. By using `SET credcheck.<check-name> TO <some value>;` command, enforce new settings for the credential checks. The settings can only be changed by a superuser.

### [Installation](#installation)

- Minimum version of PostgreSQL required is 10.0.
- For the Password Reuse Policy feature the minimum version required is 12.0.
- Make sure the `pg_config` binary is set in the current `PATH`.
- Clone or download this repository into a directory, and run the `make install` command.
- If there are any permission issues, then use the `sudo make install` command.
- Perform the regression tests by running the `make installcheck` command.
- Test this extension in one session by using `LOAD 'credcheck';` PostgreSQL command.
- To enable this extension for all the sessions, then execute `CREATE EXTENSION credcheck;` command.
- And append `credcheck` to `shared_preload_libraries` configuration parameter which is present in `postgresql.conf`.
- Then restart the PostgreSQL database to reflect the changes.


### [Checks](#checks)

Please find the below list of general checks, which we can enforce on credentials.

| Check                     | Type     | Description                                         | Setting Value | Accepted                    | Not Accepted                 |
|---------------------------|----------|-----------------------------------------------------|---------------|-----------------------------|------------------------------|
| username_min_length       | username | minimum length of a username                        | 4             | &check; abcd                | &#10008; abc                 |
| username_min_special      | username | minimum number of special characters                | 1             | &check; a@bc                | &#10008; abcd                |
| username_min_digit        | username | minimum number of digits                            | 1             | &check; a1bc                | &#10008; abcd                |
| username_min_upper        | username | minimum number of upper case                        | 2             | &check; aBC                | &#10008; aBc                |
| username_min_lower        | username | minimum number of lower case                        | 1             | &check; aBC                | &#10008; ABC                |
| username_min_repeat       | username | maximum number of times a character should repeat   | 2             | &check; aaBCa              | &#10008; aaaBCa             |
| username_contain_password | username | username should not contain password                | on            | &check; username - password | &#10008; username + password |
| username_contain          | username | username should contain one of these characters     | a,b,c         | &check; ade                 | &#10008; efg                 |
| username_not_contain      | username | username should not contain one of these characters | x,y,z         | &check; ade                 | &#10008; axf                 |
| username_ignore_case      | username | ignore case while performing the above checks       | on            | &check; Ade                 | &#10008; aXf                 |
| password_min_length       | password | minimum length of a password                        | 4             | &check; abcd                | &#10008; abc                 |
| password_min_special      | password | minimum number of special characters                | 1             | &check; a@bc                | &#10008; abc                 |
| password_min_digit        | password | minimum number of digits in a password              | 1             | &check; a1bc                | &#10008; abc                 |
| password_min_upper        | password | minimum number of uppercase characters              | 1             | &check; Abc                 | &#10008; abc                 |
| password_min_lower        | password | minimum number of lowercase characters              | 1             | &check; aBC                 | &#10008; ABC                 |
| password_min_repeat       | password | maximum number of times a character should repeat   | 2             | &check; aab                 | &#10008; aaab                |
| password_contain_username | password | password should not contain password                | on            | &check; password - username | &#10008; password + username |
| password_contain          | password | password should contain these characters            | a,b,c         | &check; ade                 | &#10008; xfg                 |
| password_not_contain      | password | password should not contain these characters        | x,y,z         | &check; abc                 | &#10008; axf                 |
| password_ignore_case      | password | ignore case while performing above checks           | on            | &check; Abc                 | &#10008; aXf                 |
| password_valid_until      | password | force use of VALID UNTIL clause in CREATE ROLE statement with a minimum number of days   | 60             | &check; CREATE ROLE abcd VALID UNTIL (now()+'3 months'::interval)::date | &#10008; CREATE ROLE abcd LOGIN; |


### [Examples](#examples)

Let us start with a simple check as every username should be of length minimum 4 characters.

```
postgres=# SHOW credcheck.username_min_length;
 credcheck.username_min_length 
-------------------------------
 4
(1 row)

postgres=# CREATE USER abc WITH PASSWORD 'pass';
ERROR:  username length should match the configured credcheck.username_min_length

postgres=# CREATE USER abcd WITH PASSWORD 'pass';
CREATE ROLE
```

Let us enforce an another check as every username should contain a special character in it.

```
postgres=# SHOW credcheck.username_min_special;
 credcheck.username_min_special 
--------------------------------
 1
(1 row)

postgres=# CREATE USER abcd WITH PASSWORD 'pass';
ERROR:  username does not contain the configured credcheck.username_min_special characters

postgres=# CREATE USER abcd$ WITH PASSWORD 'pass';
CREATE ROLE
```

Let us add one more check to the username, where username should not contain more than 1 adjacent repeat character.


```
postgres=# show credcheck.username_min_repeat ;
 credcheck.username_min_repeat 
-------------------------------
 1
(1 row)

postgres=# CREATE USER week$ WITH PASSWORD 'pass';
ERROR:  username characters are repeated more than the configured credcheck.username_min_repeat times

postgres=# CREATE USER weak$ WITH PASSWORD 'pass';
CREATE ROLE

postgres=# SHOW credcheck.username_min_repeat ;
 credcheck.username_min_repeat 
-------------------------------
 2
(1 row)

postgres=# CREATE USER week$ WITH PASSWORD 'pass';
CREATE ROLE
```

Now, let us add some checks for the password.
Let us start with a check as a password should not contain these characters (!@=$#).

```
postgres=# SHOW credcheck.password_not_contain ;
 credcheck.password_not_contain 
--------------------------------
 !@=$#
(1 row)

postgres=# CREATE USER abcd$ WITH PASSWORD 'p@ss';
ERROR:  password does contain the configured credcheck.password_not_contain characters

postgres=# CREATE USER abcd$ WITH PASSWORD 'pass';
CREATE ROLE
```

Let us add another check for the password as, the password should not contain username.

```
postgres=# SHOW credcheck.password_contain_username ;
 credcheck.password_contain_username 
-------------------------------------
 on
(1 row)

postgres=# CREATE USER abcd$ WITH PASSWORD 'abcd$xyz';
ERROR:  password should not contain username

-- OK, ignore case is disabled
postgres=# CREATE USER abcd$ WITH PASSWORD 'ABCD$xyz';
CREATE ROLE

postgres=# CREATE USER abcd$ WITH PASSWORD 'axyz';
CREATE ROLE
```

Let us make checks as to ignore the case.

```
postgres=# SHOW credcheck.password_ignore_case;
 credcheck.password_ignore_case 
--------------------------------
 on
(1 row)

postgres=# CREATE USER abcd$ WITH PASSWORD 'ABCD$xyz';
ERROR:  password should not contain username

postgres=# CREATE USER abcd$ WITH PASSWORD 'A$xyz';
CREATE ROLE
```

Let us add one final check to the password as the password should not contain any adjacent repeated characters.

```
postgres=# SHOW credcheck.password_min_repeat ;
 credcheck.password_min_repeat 
-------------------------------
 3
(1 row)

postgres=# CREATE USER abcd$ WITH PASSWORD 'straaaangepaasssword';
ERROR:  password characters are repeated more than the configured credcheck.password_min_repeat times

postgres=# CREATE USER abcd$ WITH PASSWORD 'straaangepaasssword';
CREATE ROLE
```

credcheck can also enforce the use of an expiration date for the password by checking option VALID UNTIL used in CREATE or ALTER ROLE.
```
postgres=# SET credcheck.password_valid_until = 30;
SET

postgres=# CREATE USER abcd$;
ERROR:  require a VALID UNTIL option with a date older than 30 days

postgres=# CREATE USER abcd$ VALID UNTIL '2022-12-21';
ERROR:  require a VALID UNTIL option with a date older than 30 days

postgres=# ALTER USER abcd$ VALID UNTIL '2022-12-21';
ERROR:  require a VALID UNTIL option with a date older than 30 days
```

### [Password reuse policy](#password-reuse-policy)

PostgreSQL supports natively password expiration, all other kinds of password policy enforcement comes with extensions.
With the credcheck extension, password can be forced to be of a certain length, contain amounts of various types of characters and be checked against the user account name itself.

But one thing was missing, there was no password reuse policy enforcement. That mean that when user were required to change their password, they could just reuse their current password!

The credcheck extension adds the "Password Reuse Policy" in release 1.0.0. To used this feature, the credcheck extension MUST be added to `shared_preload_libraries` configuration option.

All users passwords are historicized in shared memory together with the timestamps of when these passwords were set. The passwords history is saved into a file named `$PGDATA/global/pg_password_history` to be reloaded in shared memory at startup. This file must be part of your backups if you don't want to loose the password history, hopefully pg_basebackup will take care of it.

Two settings allow to control the behavior of this feature:

* `credcheck.password_reuse_history`: number of distinct passwords set before a password can be reused.
* `credcheck.password_reuse_interval`: amount of time it takes before a password can be reused again.

The default value for these settings are 0 which means that all password reuse policies are disabled.

The password history consists of passwords a user has been assigned in the past. credcheck can
restrict new passwords from being chosen from this history:

* If an account is restricted on the basis of number of password changes, a new password cannot be chosen from the `password_reuse_history` most recent passwords. For example, minimum number of password changes is set to 3, a new password cannot be the same as any of the most recent 3 passwords.

* If an account is restricted based on time elapsed, a new password cannot be chosen from passwords in the history that are newer than `password_reuse_interval` days. For example, if the password reuse interval is set to 365, a new password must not be among those previously chosen within the last year. 

To be able to list the content of the history a view is provided in the database you have created
the credcheck extension. The view is named `public.pg_password_history`. This view is visible by everyone.

A superuser can also reset the content of the password history by calling a function named `public.pg_password_history_reset()`. If it is called without an argument, all the passords history will be cleared. To only remove the records registered for a single user, just pass his name as parameter. This function returns the number of records removed from the history.

Example:
```
SET credcheck.password_reuse_history = 2;
CREATE USER credtest WITH PASSWORD 'H8Hdre=S2';
ALTER USER credtest PASSWORD 'J8YuRe=6O';
SELECT rolename, password_hash FROM pg_password_history WHERE rolename = 'credtest' ORDER BY password_date;
 rolename |                          password_hash
----------+------------------------------------------------------------------
 credtest | 7488570b80076cf9da26644d5eeb316c4768ff5bee7bf319344e7bb328032098
 credtest | e61e58c22aa6bf31a92b385932f7d0e4dbaba24fa3fdb2982510d6c72a961335
(2 rows)

-- fail, the credential is still in the history
ALTER USER credtest PASSWORD 'J8YuRe=6O';
ERROR:  Cannot use this credential following the password reuse policy

-- Reset the password history
SELECT pg_password_history_reset();
 pg_password_history_reset
---------------------------
                         2
(1 row)
```

Example for password reuse interval:
```
SET credcheck.password_reuse_history = 1;
SET credcheck.password_reuse_interval = 365;
-- Add a new password in the history and set its age to 100 days
ALTER USER credtest PASSWORD 'J8YuRe=6O';
SELECT pg_password_history_timestamp('credtest', now()::timestamp - '100 days'::interval);
 pg_password_history_timestamp
-------------------------------
                             1
(1 row)

SELECT * FROM pg_password_history WHERE rolename = 'credtest';
 rolename |         password_date         |                          password_hash                           
----------+-------------------------------+------------------------------------------------------------------
 credtest | 2022-12-15 13:41:06.736775+03 | c38cf85ca6c3e5ee72c09cf0bfb42fb29b0f0a3e8ba335637941d60f86512508
(1 row)

-- fail, the password is in the history for less than 1 year
ALTER USER credtest PASSWORD 'J8YuRe=6O';
ERROR:  Cannot use this credential following the password reuse policy
-- Change the age of the password to exceed the 1 year interval
SELECT pg_password_history_timestamp('credtest', now()::timestamp - '380 days'::interval);
 pg_password_history_timestamp
-------------------------------
                             2
(1 row)

-- success, the old password present in the history has expired and will be removed
ALTER USER credtest PASSWORD 'J8YuRe=6O';
SELECT rolename, password_hash FROM pg_password_history WHERE rolename = 'credtest';
 rolename |         password_date         |                          password_hash                           
----------+-------------------------------+------------------------------------------------------------------
 credtest | 2023-03-25 13:42:37.387629+03 | c38cf85ca6c3e5ee72c09cf0bfb42fb29b0f0a3e8ba335637941d60f86512508
(1 row)
```

Function `pg_password_history_timestamp()` is provided for testing purpose only and allow a superuer
to change the timestamp of all registered passwords in the history.

### [Limitations](#limitations)

This extension only works for the plain text passwords.

Example
```
postgres=# CREATE USER user1 PASSWORD 'this is some plain text';
CREATE ROLE
```
An error will report, if any user trying to create user with an ENCRYPTED password.

Example
```   
postgres=# CREATE USER user1 PASSWORD 'md55e4cc86d2d6a8b73bbefc4d5b91baa45';
ERROR:  password type is not a plain text
```

Username checks will not get enforced while create an user without password, and while renaming the user if the user doesn't have a password defined.

Example (username checks won't invoke here)
```
postgres=# CREATE USER user1;
```

Example (username checks won't invoke here)
```
postgres=# ALTER USER user1 RENAME to test_user;
```

Example (username checks will invoke here and on the rename statement too)
```
postgres=# CREATE USER user1 PASSWORD 'this is some plain text';
CREATE ROLE
postgres=# ALTER USER user1 RENAME to test_user;
```

### [Authors](#authors)

- Dinesh Kumar
- Gilles Darold

Maintainer: Gilles Darold

### [License](#license)

This extension is free software distributed under the PostgreSQL License.

    Copyright (c) 2021-2023 MigOps Inc.

### [Credits](#credits)

- Thanks to the [passwordcheck](https://www.postgresql.org/docs/current/passwordcheck.html) extension author
- Thanks to the [password policy](https://github.com/eendroroy/passwordpolicy) extension author
- Thanks to the [blog author](https://paquier.xyz/postgresql-2/postgres-module-highlight-customize-passwordcheck-to-secure-your-database/) Mickael Paquier
