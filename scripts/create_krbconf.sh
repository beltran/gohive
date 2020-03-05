#!/bin/bash

cat >/etc/krb5.conf <<EOL
[libdefaults]
 default_realm = EXAMPLE.COM
 default_tkt_enctypes = des3-hmac-sha1 des-cbc-crc
 default_tgs_enctypes = des3-hmac-sha1 des-cbc-crc
 dns_lookup_kdc = true
 dns_lookup_realm = true

[realms]
 EXAMPLE.COM = {
     kdc = kerberos.example.com
     admin_server = kerberos.example.com
     default_domain = EXAMPLE.COM
 }

[domain_realm]
 .example.com = EXAMPLE.COM
 example.com = EXAMPLE.COM

[logging]
 kdc = SYSLOG:INFO
 admin_server = FILE=kerberos.log

EOL
