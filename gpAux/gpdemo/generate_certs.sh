#!/bin/sh

csr="server.req"
key="server.key"
cert="server.crt"

# create the csr
openssl req -new -passin pass:password -passout pass:password -text -out $csr 2>&1 <<-EOF
US
California
Palo Alto
Pivotal
GPDB
localhost
gpdb@127.0.0.1
.
.
EOF

[ -f ${csr} ] && openssl req -text -noout -in ${csr} 2>&1

# create the key
openssl rsa -in privkey.pem -passin pass:password -passout pass:password -out ${key}

# create the certificate
openssl x509 -in ${csr} -out ${cert} -req -signkey ${key} -days 1000

chmod og-rwx ${key}

mkdir -p certificate/gpfdists

cp $key certificate/gpfdists/server.key
cp $cert certificate/gpfdists/server.crt
cp $key certificate/gpfdists/client.key
cp $cert certificate/gpfdists/client.crt
cp $cert certificate/gpfdists/root.crt

rm -f server.* privkey.pem
