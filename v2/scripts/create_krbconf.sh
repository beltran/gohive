#!/bin/bash

cat >/etc/krb5.conf <<EOL
[libdefaults]
 default_realm = EXAMPLE.COM
 default_tkt_enctypes = des3-hmac-sha1 des-cbc-crc rc4-hmac  des-cbc-md5
 default_tgs_enctypes = des3-hmac-sha1 des-cbc-crc rc4-hmac  des-cbc-md5
 permitted_enctypes = des3-hmac-sha1 des-cbc-crc rc4-hmac  des-cbc-md5
 dns_lookup_kdc = true
 dns_lookup_realm = true
 allow_weak_crypto = true

[realms]
 EXAMPLE.COM = {
     kdc = kerberos.example.com
     admin_server = kerberos.example.com
 }

[domain_realm]
 .example.com = EXAMPLE.COM
 example.com = EXAMPLE.COM

[logging]
 kdc = SYSLOG:INFO
 admin_server = FILE=kerberos.log

EOL
