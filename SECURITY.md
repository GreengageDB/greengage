# Security Release Process

Greengage Database has adopted this security disclosure and response policy to
ensure we responsibly handle critical issues.

## Reporting a Vulnerability - Private Disclosure Process

Security is of the highest importance and all security vulnerabilities or
suspected security vulnerabilities should be reported to Greengage Database
privately, to minimize attacks against current users of Greengage Database
before they are fixed. Vulnerabilities will be investigated and patched on the
next patch (or minor) release as soon as possible. This information could be
kept entirely internal to the project.

If you know of a publicly disclosed security vulnerability for Greengage
Database, please **IMMEDIATELY** contact the Greengage Database project team
(security@greengagedb.org).

**IMPORTANT: Do not file public issues on GitHub for security vulnerabilities!**

To report a vulnerability or a security-related issue, please contact the email
address with the details of the vulnerability. The email will be fielded by the
Greengage Database project team. Emails will be addressed promptly, including a
detailed plan to investigate the issue and any potential workarounds to perform
in the meantime. Do not report non-security-impacting bugs through this
channel. Use [GitHub issues](https://github.com/GreengageDB/greengage/issues)
instead.

## Proposed Email Content

Provide a descriptive subject line and in the body of the email include the
following information:

* Basic identity information, such as your name and your affiliation or company.
* Detailed steps to reproduce the vulnerability  (POC scripts, screenshots, and
  logs are all helpful to us).
* Description of the effects of the vulnerability on Greengage Database and the
  related hardware and software configurations, so that the Greengage Database
  project team can reproduce it.
* How the vulnerability affects Greengage Database usage and an estimation of
  the attack surface, if there is one.
* List other projects or dependencies that were used in conjunction with
  Greengage Database to produce the vulnerability.

## When to report a vulnerability

* When you think Greengage Database has a potential security vulnerability.
* When you suspect a potential vulnerability but you are unsure that it impacts
  Greengage Database.
* When you know of or suspect a potential vulnerability on another project that
  is used by Greengage Database.

## Patch, Release, and Disclosure

The Greengage Database project team will respond to vulnerability reports as
follows:

1. The Greengage project team will investigate the vulnerability and determine
its effects and criticality.
2. If the issue is not deemed to be a vulnerability, the Greengage project team
will follow up with a detailed reason for rejection.
3. The Greengage project team will initiate a conversation with the reporter
promptly.
4. If a vulnerability is acknowledged and the timeline for a fix is determined,
the Greengage project team will work on a plan to communicate with the
appropriate community, including identifying mitigating steps that affected
users can take to protect themselves until the fix is rolled out.
5. The Greengage project team will also create a
[CVSS](https://www.first.org/cvss/specification-document) using the [CVSS
Calculator](https://www.first.org/cvss/calculator/3.0). The Greengage project
team makes the final call on the calculated CVSS; it is better to move quickly
than making the CVSS perfect. Issues may also be reported to
[Mitre](https://cve.mitre.org/) using this [scoring
calculator](https://nvd.nist.gov/vuln-metrics/cvss/v3-calculator). The CVE will
initially be set to private.
6. The Greengage project team will work on fixing the vulnerability and perform
internal testing before preparing to roll out the fix.
7. A public disclosure date is negotiated by the Greengage Database project
team, and the bug submitter. We prefer to fully disclose the bug as soon as
possible once a user mitigation or patch is available. It is reasonable to
delay disclosure when the bug or the fix is not yet fully understood, or the
solution is not well-tested. The timeframe for disclosure is from immediate
(especially if it’s already publicly known) to a few weeks. The Greengage
Database project team holds the final say when setting a public disclosure
date.
8. Once the fix is confirmed, the Greengage project team will patch the
vulnerability in the next patch or minor release, and backport a patch release
into earlier supported releases as necessary. Upon release of the patched
version of Greengage Database, we will follow the **Public Disclosure
Process**.

## Public Disclosure Process

The Greengage project team publishes a [public
advisory](https://github.com/GreengageDB/greengage/security/advisories?state=published)
to the Greengage Database community via GitHub. In most cases, additional
communication via Slack, Twitter, mailing lists, blog and other channels will
assist in educating Greengage Database users and rolling out the patched
release to affected users.

The Greengage project team will also publish any mitigating steps users can
take until the fix can be applied to their Greengage Database instances.

## Mailing lists

* Use security@greengagedb.org to report security concerns to the Greengage
  Database project team, who uses the list to privately discuss security issues
  and fixes prior to disclosure.

## Confidentiality, integrity and availability

We consider vulnerabilities leading to the compromise of data confidentiality,
elevation of privilege, or integrity to be our highest priority concerns.
Availability, in particular in areas relating to DoS and resource exhaustion,
is also a serious security concern. The Greengage Database project team takes
all vulnerabilities, potential vulnerabilities, and suspected vulnerabilities
seriously and will investigate them in an urgent and expeditious manner.

Note that we do not currently consider the default settings for Greengage
Database to be secure-by-default. It is necessary for operators to explicitly
configure settings, role based access control, and other resource related
features in Greengage Database to provide a hardened Greengage Database
environment. We will not act on any security disclosure that relates to a lack
of safe defaults. Over time, we will work towards improved safe-by-default
configuration, taking into account backwards compatibility.
