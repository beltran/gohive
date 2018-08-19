#!/bin/bash

set -eux

function wait_for_hive () {
    # Wait for port to open
    counter=0
    while [ 1 ]
    do
        curl hs2.example.com:10000
        if [ $? -eq 0 ]; then
          break
        fi
        counter=$((counter+1))
        if [[ "$counter" -gt 12 ]]; then
          # Just fail because the port didn't open
          echo "Waited for two minutes and hive didn't appear to start"
          exit 1
        fi
        echo "Waiting for hive port to open"
        sleep 10
    done
}

function install_deps() {
    if [ ! -d "dhive" ] ; then
        git clone https://github.com/beltran/dhive
    else 
        cd dhive
        git fetch && git reset --hard origin/master
        cd ..
    fi
    pushd dhive
    pip install --user -r requirements.txt
    sed -i.bak 's/hive_version.*/hive_version = 3.1.0/g' ./config/hive.cfg
    sed -i.bak 's/hive_version.*/hive_version = 3.1.0/g' ./config/hive_and_kerberos.cfg
    sed -i.bak 's/hive.server2.thrift.sasl.qop.*/hive.server2.thrift.sasl.qop = auth-conf/g' ./config/hive_and_kerberos.cfg
    popd
}

function setHttpTransport() {
    pushd dhive
    echo "hive.server2.transport.mode = http" >> ./config/hive_and_kerberos.cfg
    echo "hive.server2.thrift.http.port = 10000" >> ./config/hive_and_kerberos.cfg
    echo "hive.server2.use.SSL = true" >> ./config/hive_and_kerberos.cfg
    echo "hive.server2.keystore.path = /var/keytabs/hive.jks" >> ./config/hive_and_kerberos.cfg
    echo "hive.server2.keystore.password = changeme" >> ./config/hive_and_kerberos.cfg
    popd
}

function setHive() {
    pushd dhive
    DHIVE_CONFIG_FILE=$1 make dclean all
    popd
}

function bringCredentials() {
    kdestroy
    docker cp kerberos.example:/var/keytabs/hdfs.keytab .
    export KRB5CCNAME=/tmp/krb5_gohive
    kinit -c $KRB5CCNAME -kt ./hdfs.keytab hive/hs2.example.com@EXAMPLE.COM
    # kinit -t ./hdfs.keytab hive/localhost@EXAMPLE.COM
    klist

    docker cp hs2.example:/var/keytabs/hive.jks .

    rm -rf /tmp/hostname-keystore.p12 client.cer.pem client.cer.key
    keytool -importkeystore -srckeystore hive.jks \
    -srcstorepass changeme -srckeypass changeme -destkeystore /tmp/hostname-keystore.p12 \
    -deststoretype PKCS12 -srcalias hs2.example.com -deststorepass changeme -destkeypass changeme

    openssl pkcs12 -in /tmp/hostname-keystore.p12 -out client.cer.pem -clcerts -nokeys -passin pass:changeme
    openssl pkcs12 -in /tmp/hostname-keystore.p12 -out client.cer.key -nocerts -nodes -passin pass:changeme
}

function run_tests() {
    
    setHive config/hive_and_kerberos.cfg
    wait_for_hive || exit 1

    # Tests with binary transport and kerberos authentication
    bringCredentials
    export TRANSPORT="binary"
    export AUTH="KERBEROS"
    export SSL="0"
    go test -v -run .

    # Tests with http transport and kerberos authentication
    setHttpTransport
    setHive config/hive_and_kerberos.cfg
    wait_for_hive || exit 1

    bringCredentials
    export TRANSPORT="http"
    export AUTH="KERBEROS"
    export SSL="1"
    go test -v -run .

    # Tests with binary transport and none authentication
    setHive config/hive.cfg
    wait_for_hive || exit 1

    export TRANSPORT="binary"
    export AUTH="NONE"
    export SSL="0"
    go test -v -run .

}

install_deps
run_tests
