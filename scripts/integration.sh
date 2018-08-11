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

function setHive() {
    pushd dhive
    DHIVE_CONFIG_FILE=$1 make dclean all
    popd
}

function run_tests() {
    
    setHive config/hive_and_kerberos.cfg
    wait_for_hive || exit 1

    export KRB5CCNAME=/tmp/krb5_gohive
    kdestroy || echo ""
    docker cp kerberos.example:/var/keytabs/hdfs.keytab .
    kinit -c $KRB5CCNAME -kt ./hdfs.keytab hive/hs2.example.com@EXAMPLE.COM
    klist

    export AUTH="KERBEROS"
    go test -v -run .

    setHive config/hive.cfg
    wait_for_hive || exit 1

    export AUTH="NONE"
    go test -v -run .

}

install_deps
run_tests
