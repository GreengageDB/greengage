## credcheck - PostgreSQL username/password checks


- [credcheck - PostgreSQL username/password checks](#credcheck---postgresql-usernamepassword-checks)
	- [Description](#description)
	- [Checks](#checks)


### [Description](#description)

The `credcheck` PostgreSQL extension provides few general credentail checks, which will be evaluated during the user creation or during the password change. By using this extension, we can define a pattern to allow a specific username/password, and a pattern to reject a certain type of credentials. This extension is developed based on the PostgreSQL's `check_password_hook` which will perform the checks on the credentials.

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
| username_not_contain      | username | username should not contain one of these characters | x,y,z         | &check; ade                 | &#10008; xfg                 |
| username_ignore_case      | username | ignore case while performing the above checks       | on            | &check; Ade                 | &#10008; Xfg                 |
| password_min_length       | password | minimum length of a password                        | 4             | &check; abcd                | &#10008; abc                 |
| password_min_special      | password | minimum number of special characters                | 1             | &check; a@bc                | &#10008; abc                 |
| password_min_digit        | password | minimum number of digits in a password              | 1             | &check; a1bc                | &#10008; abc                 |
| password_min_upper        | password | minimum number of uppercase characters              | 1             | &check; Abc                 | &#10008; abc                 |
| password_min_lower        | password | minimum number of lowercase characters              | 1             | &check; aBC                 | &#10008; ABC                 |
| password_min_repeat       | password | minimum number of times a character should repeat   | 2             | &check; aab                 | &#10008; aaab                |
| password_contain_username | password | password should not contain password                | on            | &check; password - username | &#10008; password + username |
| password_contain          | password | password should contain these characters            | a,b,c         | &check; ade                 | &#10008; efg                 |
| password_not_contain      | password | password should not contain these characters        | x,y,z         | &check; abc                 | &#10008; xfg                 |
| password_ignore_case      | password | ignore case while performing above checks           | on            | &check; Abc                 | &#10008; Xfg                 |

