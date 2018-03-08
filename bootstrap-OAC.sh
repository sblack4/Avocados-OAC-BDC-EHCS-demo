#!/bin/bash

# Use this script on OAC
# Just add BDC IP
# this shell script just sets up OAC
# to connect to BDC for the Pandora POC

echo "Running bootstrap to connect BDC to OAC"

bdc_ip=129.150.115.185

# if you forgot to put in the bdc address
# we're going to have to do this later...
if [[ -z "$bdc_ip" ]]; then 
  echo "oops... no bdc ip means no cert..."
  cp $0 /home/opc
  exit 0
fi

# get java home
# and set paths to necessary tools
OAC_JDK_PATH="/u01/app/[-0-9\.]*/jdk"
OAC_JDK=$(eval echo $OAC_JDK_PATH)
KEYTOOL="${OAC_JDK}"/bin/keytool
CACERTS=$OAC_JDK/jre/lib/security/cacerts

echo "jdk path: $OAC_JDK"
echo "keytool: $KEYTOOL"
echo "cacerts: $CACERTS"

# get BDCs nginx.crt
openssl s_client -showcerts -connect $bdc_ip:1080 </dev/null 2>/dev/null | openssl x509 -text > nginx.crt

# add key to keystore
$KEYTOOL -import -trustcacerts \
  -keystore $CACERTS \
  -storepass changeit \
  -trustcacerts \
  -noprompt \
  -alias bdcsce \
  -file nginx.crt

echo "done!"
