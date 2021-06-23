## credcheck - PostgreSQL username/password checks



- [credcheck - PostgreSQL username/password checks](#credcheck---postgresql-usernamepassword-checks)
	- [Description](#description)
	- [Installation](#installation)
	- [Checks](#checks)
	- [Examples](#examples)
	- [Limitations](#limitations)
	- [Authors](#authors)
	- [License](#license)
	- [Credits](#credits)



### [Description](#description)

The `credcheck` PostgreSQL extension provides few general credentail checks, which will be evaluated during the user creation or during the password change. By using this extension, we can define a set of rules to allow a specific set of credentials, and a set of rules to reject a certain type of credentials. This extension is developed based on the PostgreSQL's `check_password_hook` hook.

This extension provides all the checks as configuration parameters, and the values can be applied during the configuration reload(SIGHUP).
When we create this extension, the default configuration settings, will not enforce any complex checks and will try to allow all the credentials. By using `ALTER SYSTEM SET credcheck.<check-name> TO <some value>;` command, followed by `SELECT pg_reload_conf();` command we can enforce new settings for the credential checks.

### [Installation](#installation)
- Make sure the `pg_config` is set in the currnet `PATH`.
- Clone or download this repository in a directory, and run the `make install` command.
- If there are any permission issues, then use the `sudo make install` command.
- Perform the regression tests by running the `make installcheck` command.
- Test this extension in one session by using `LOAD 'credcheck';` PostgreSQL command.
- To enable this extension for all the sessions, then execute `CREATE EXTENSION credcheck;` command.
- And append `credcheck` to `shared_preload_libraries` configuration parameter which is present in `postgresql.conf`.
- Then restart the PostgreSQL database to reflect the changes.


### [Checks](#checks)
| Check                     | Type     | Description                                         | Setting Value | Accepted                    | Not Accepted                 |
|---------------------------|----------|-----------------------------------------------------|---------------|-----------------------------|------------------------------|
| username_min_length       | username | minimum length of a username                        | 4             | &check; abcd                | &#10008; abc                 |
| username_min_special      | username | minimum number of special characters                | 1             | &check; a@bc                | &#10008; abcd                |
| username_min_digit        | username | minimum number of digits                            | 1             | &check; a1bc                | &#10008; abcd                |
| username_min_upper        | username | minimum number of upper case                        | 2             | &check; aBC                | &#10008; aBc                |
| username_min_lower        | username | minimum number of lower case                        | 1             | &check; aBC                | &#10008; ABC                |
| username_min_repeat       | username | minimum number of times a character should repeat   | 2             | &check; aaBCa              | &#10008; aaaBCa             |
| username_contain_password | username | username should not contain password                | on            | &check; username - password | &#10008; username + password |
| username_contain          | username | username should contain one of these characters     | a,b,c         | &check; ade                 | &#10008; efg                 |
| username_not_contain      | username | username should not contain one of these characters | x,y,z         | &check; ade                 | &#10008; axf                 |
| username_ignore_case      | username | ignore case while performing the above checks       | on            | &check; Ade                 | &#10008; aXf                 |
| password_min_length       | password | minimum length of a password                        | 4             | &check; abcd                | &#10008; abc                 |
| password_min_special      | password | minimum number of special characters                | 1             | &check; a@bc                | &#10008; abc                 |
| password_min_digit        | password | minimum number of digits in a password              | 1             | &check; a1bc                | &#10008; abc                 |
| password_min_upper        | password | minimum number of uppercase characters              | 1             | &check; Abc                 | &#10008; abc                 |
| password_min_lower        | password | minimum number of lowercase characters              | 1             | &check; aBC                 | &#10008; ABC                 |
| password_min_repeat       | password | minimum number of times a character should repeat   | 2             | &check; aab                 | &#10008; aaab                |
| password_contain_username | password | password should not contain password                | on            | &check; password - username | &#10008; password + username |
| password_contain          | password | password should contain these characters            | a,b,c         | &check; ade                 | &#10008; xfg                 |
| password_not_contain      | password | password should not contain these characters        | x,y,z         | &check; abc                 | &#10008; axf                 |
| password_ignore_case      | password | ignore case while performing above checks           | on            | &check; Abc                 | &#10008; aXf                 |


### [Examples](#examples)

Let us start with a simple check as every username should be of length minimum 4 characters.

```
postgres=# SHOW credcheck.username_min_length;
 credcheck.username_min_length 
-------------------------------
 4
(1 row)

-- ERROR
postgres=# CREATE USER abc WITH PASSWORD 'pass';
ERROR:  username length should match the configured credcheck.username_min_length

-- OK
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

-- ERROR
postgres=# CREATE USER abcd WITH PASSWORD 'pass';
ERROR:  username does not contain the configured credcheck.username_min_special characters

-- OK
postgres=# CREATE USER abcd$ WITH PASSWORD 'pass';
CREATE ROLE
```

Let us add one more check to the username, where username should not contain more than 1 adjacent repeat characters.


```
postgres=# show credcheck.username_min_repeat ;
 credcheck.username_min_repeat 
-------------------------------
 1
(1 row)

-- ERROR
postgres=# CREATE USER week$ WITH PASSWORD 'pass';
ERROR:  username characters are repeated more than the configured credcheck.username_min_repeat times

-- OK
postgres=# CREATE USER weak$ WITH PASSWORD 'pass';
CREATE ROLE

postgres=# SHOW credcheck.username_min_repeat ;
 credcheck.username_min_repeat 
-------------------------------
 2
(1 row)

-- OK
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

-- ERROR
postgres=# CREATE USER abcd$ WITH PASSWORD 'p@ss';
ERROR:  password does contain the configured credcheck.password_not_contain characters

-- OK
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

-- ERROR
postgres=# CREATE USER abcd$ WITH PASSWORD 'abcd$xyz';
ERROR:  password should not contain username

-- OK, ignore case is disabled
postgres=# CREATE USER abcd$ WITH PASSWORD 'ABCD$xyz';
CREATE ROLE

-- OK
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

-- ERROR
postgres=# CREATE USER abcd$ WITH PASSWORD 'ABCD$xyz';
ERROR:  password should not contain username

-- OK
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

-- ERROR
postgres=# CREATE USER abcd$ WITH PASSWORD 'straaaangepaasssword';
ERROR:  password characters are repeated more than the configured credcheck.password_min_repeat times

-- OK
postgres=# CREATE USER abcd$ WITH PASSWORD 'straaangepaasssword';
CREATE ROLE
```


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
Username checks will not get enforce while renaming the user.
  
Example
```
postgres=# ALTER USER user1 RENAME to test_user;
```





### [Authors](#authors)
- Dinesh Kumar

### [License](#license)

This extension is free software distributed under the PostgreSQL
License.

    Copyright (c) 2021 MigOps Inc.
### [Credits](#credits)
- Thanks to the [passwordcheck extension author](https://www.postgresql.org/docs/current/passwordcheck.html)
- Thanks to the [password policy extension author](https://github.com/eendroroy/passwordpolicy)
- Thanks to the [blog author](https://paquier.xyz/postgresql-2/postgres-module-highlight-customize-passwordcheck-to-secure-your-database/)