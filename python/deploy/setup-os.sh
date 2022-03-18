#!/bin/bash -eux

# Copied from the link below with minor modifications:
# https://github.com/implydata/distribution-docker/blob/master/setup-os.sh

#
# Base OS stuff
#

apt-get update
apt-get -y upgrade
apt-get -y install --no-install-recommends \
                   curl apt-transport-https \
                   python python2.7 perl \
                   software-properties-common gnupg2 jq vim less wget

#
# Sync Java (Azul Zulu) repository
#

apt-key add - <<'EOT'
-----BEGIN PGP PUBLIC KEY BLOCK-----
Version: SKS 1.1.6
Comment: Hostname: keyserver.ubuntu.com

mQINBFNgFa8BEADTL/REB10M+TfiZOtFHqL5LHKkzTMn/O2r5iIqXGhi6iwZazFs9S5g1eU7
WMen5Xp9AREs+OvaHx91onPZ7ZiP7VpZ6ZdwWrnVk1Y/HfI59tWxmNYWDmKYBGMj4EUpFPSE
9EnFj7dm1WdlCvpognCwZQl9D3BseGqN7OLHfwqqmOlbYN9hHYkT+CaqOoWDIGMB3UkBlMr0
GuujEP8N1gxg7EOcSCsZH5aKtXubdUlVSphfAAwDz4MviB39J22sPBnKmaOT3TUTO5vGeKtC
9BAvtgA82jY2TtCEjetnfK/qtzj/6j2NxVUbHQydwNQVRU92A7334YvCbn3xUUNI0WOscdmf
pgCU0Z9Gb2IqDb9cMjgUi8F6MG/QY9/CZjX62XrHRPm3aXsCJOVh/PO1sl2A/rvv8AkpJKYy
hm6T8OBFptCsA3V4Oic7ZyYhqV0u2r4NON+1MoUeuuoeY2tIrbRxe3ffVOxPzrESzSbc8LC2
tYaP+wGdW0f57/CoDkUzlvpReCUI1Bv5zP4/jhC63Rh6lffvSf2tQLwOsf5ivPhUtwUfOQjg
v9P8Wc8K7XZpSOMnDZuDe9wuvB/DiH/P5yiTs2RGsbDdRh5iPfwbtf2+IX6h2lNZXiDKt9Gc
26uzeJRx/c7+sLunxq6DLIYvrsEipVI9frHIHV6fFTmqMJY6SwARAQABtEdBenVsIFN5c3Rl
bXMsIEluYy4gKFBhY2thZ2Ugc2lnbmluZyBrZXkuKSA8cGtpLXNpZ25pbmdAYXp1bHN5c3Rl
bXMuY29tPokCOAQTAQIAIgUCU2AVrwIbAwYLCQgHAwIGFQgCCQoLBBYCAwECHgECF4AACgkQ
sZmDYSGb2cnJ8xAAz1V1PJnfOyaRIP2NHo2uRwGdPsA4eFMXb4Z08eGjDMD3b9WW3D0XnCLb
JpaZ6klz0W0s2tcYSneTBaSsRAqxgJgBZ5ZMXtrrHld/5qFoBbStLZLefmcPhnfvamwHDCTL
Uex8NIAI1u3e9Rhb5fbH+gpuYpwHX7hz0FOfpn1sxR03UyxU+ey4AdKe9LG3TJVnB0Wcgxpo
bpbqweLHyzcEQCNoFV3r1rlE13Y0aE31/9apoEwiYvqAzEmE38TukDLl/Qg8rkR1t0/lok2P
G6pWqdN7pmoUovBTvDi5YOthcjZcdOTXXn2Yw4RZVF9uhRsVfku1Eg25SnOje3uYsmtQLME4
eESbePdjyV/okCIle66uHZse+7gNyNmWpf01hM+VmAySIAyKa0Ku8AXZMydEcJTebrNfW9uM
LsBx3Ts7z/CBfRng6F8louJGlZtlSwddTkZVcb26T20xeo0aZvdFXM2djTi/a5nbBoZQL85A
EeV7HaphFLdPrgmMtS8sSZUEVvdaxp7WJsVuF9cONxsvx40OYTvfco0W41Lm8/sEuQ7YueEV
pZxiv5kX56GTU9vXaOOi+8Z7Ee2w6Adz4hrGZkzztggs4tM9geNYnd0XCdZ/ICAskKJABg7b
iDD1PhEBrqCIqSE3U497vibQMpkkl/Zpp0BirhGWNyTg8K4JrsQ=
=d320
-----END PGP PUBLIC KEY BLOCK-----
EOT

echo "deb http://repos.azulsystems.com/debian stable main" > /etc/apt/sources.list.d/zulu.list

#
# Java
#

apt-get update
apt-get -y install zulu-8=8.44.0.11

#
# Housekeeping
#

apt-get -y remove software-properties-common 
apt-get -y autoremove
apt-get -y clean
rm -fr /tmp/*
