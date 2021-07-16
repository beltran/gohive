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
        if [[ "$counter" -gt 18 ]]; then
          # Just fail because the port didn't open
          echo "Waited for three minutes and hive didn't appear to start"
          docker logs hs2.example
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
    #git reset --hard 750e11b9c07d79d97aea1e182ef12965fc4e922d
    sed -i.bak 's/python3/python3.6/g' ./Makefile

    python3.6 -m pip install --user -r requirements.txt
    sed -i.bak 's/hive_version.*/hive_version = 3.1.2/g' ./config/hive.cfg
    sed -i.bak 's/hive_version.*/hive_version = 3.1.2/g' ./config/hive_and_metastore_and_kerberos.cfg
    sed -i.bak 's/hadoop_version.*/hadoop_version = 2.10.1/g' ./config/hive.cfg
    sed -i.bak 's/hadoop_version.*/hadoop_version = 2.10.1/g' ./config/hive_and_metastore_and_kerberos.cfg
    sed -i.bak 's/hive.server2.thrift.sasl.qop.*/hive.server2.thrift.sasl.qop = auth-conf/g' ./config/hive_and_metastore_and_kerberos.cfg
    popd
}

function setHttpTransport() {
    pushd dhive
    echo "hive.server2.transport.mode = http" >> ./config/hive_and_metastore_and_kerberos.cfg
    echo "hive.server2.thrift.http.port = 10000" >> ./config/hive_and_metastore_and_kerberos.cfg
    echo "hive.server2.use.SSL = true" >> ./config/hive_and_metastore_and_kerberos.cfg
    echo "hive.server2.keystore.path = /var/keytabs/hive.jks" >> ./config/hive_and_metastore_and_kerberos.cfg
    echo "hive.server2.keystore.password = changeme" >> ./config/hive_and_metastore_and_kerberos.cfg
    popd
}

function setHive() {
    pushd dhive
    DHIVE_CONFIG_FILE=$1 make dclean all
    popd
}

function tearDown() {
    pushd dhive
    DHIVE_CONFIG_FILE=config/hive_and_metastore_and_kerberos.cfg make dclean
    popd
}

function bringCredentials() {
    kdestroy || true
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

function  binaryKerberos() {
  # Tests with binary transport and kerberos authentication
  setHive config/hive_and_metastore_and_kerberos.cfg
  wait_for_hive || { echo 'Failed waiting for hive' ; exit 1; }

  bringCredentials
  export TRANSPORT="binary"
  export AUTH="KERBEROS"
  export SSL="0"
  go test -tags "integration kerberos" -race -v -run . || { echo "Failed TRANSPORT=$TRANSPORT, AUTH=$AUTH, SSL=$SSL" ; docker logs hs2.example ; exit 2; }
  go test -tags "integration kerberos" -covermode=count -coverprofile=a.part -v -run . || { echo "Failed TRANSPORT=$TRANSPORT, AUTH=$AUTH, SSL=$SSL" ; docker logs hs2.example ; exit 2; }
  go run -tags "kerberos" example/main.go
}

function httpKerberos() {
  # Tests with http transport and kerberos authentication
  setHttpTransport
  setHive config/hive_and_metastore_and_kerberos.cfg
  wait_for_hive || exit 1

  bringCredentials
  export TRANSPORT="http"
  export AUTH="KERBEROS"
  export SSL="1"
  # go test -tags "integration kerberos" -race -v -run . || { echo "Failed TRANSPORT=$TRANSPORT, AUTH=$AUTH, SSL=$SSL" ; docker logs hs2.example ; exit 2; }
  go test -tags "integration kerberos" -covermode=count -coverprofile=b.part -v -run . || { echo "Failed TRANSPORT=$TRANSPORT, AUTH=$AUTH, SSL=$SSL" ; docker logs hs2.example ; exit 2; }
}

function binaryNone() {
  # Tests with binary transport and none authentication
  setHive config/hive.cfg
  wait_for_hive || exit 1

  export TRANSPORT="binary"
  export AUTH="NONE"
  export SSL="0"
  # go test -tags integration -race -v -run . || { echo "Failed TRANSPORT=$TRANSPORT, AUTH=$AUTH, SSL=$SSL" ; docker logs hs2.example ; exit 2; }
  go test -tags integration -covermode=count -coverprofile=c.part -v -run . || { echo "Failed TRANSPORT=$TRANSPORT, AUTH=$AUTH, SSL=$SSL" ; docker logs hs2.example ; exit 2; }

  echo "mode: count" >coverage.out
  grep -h -v "mode: count" *.part >>coverage.out
}

function run_tests() {

    export SKIP_UNSTABLE="1"

    binaryKerberos
    httpKerberos
    binaryNone

    goveralls -coverprofile=coverage.out -service=travis-ci

    tearDown
}

install_deps
run_tests
