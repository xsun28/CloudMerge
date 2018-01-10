#!/bin/bash
##install Java 8
cd jdk1.8.0_151/
sudo alternatives --install /usr/bin/java java /home/ec2-user/volume/jdk1.8.0_151/bin/java 2
sudo alternatives --install /usr/bin/javac javac /home/ec2-user/volume/jdk1.8.0_151/bin/javac 2
cd -
##install tabix
sudo yum install gcc
sudo yum install zlib-devel
sudo yum install bzip2-devel
sudo yum install xz-devel.x86_64

####
cat << EOF >> ~/.bashrc
export inputDir=input/
export outputDir=Results/
export startChr=1
export endChr=26
export procnum=16
export chr_prefix=chr
export PATH=/home/ec2-user/volume/htslib/bin/:$PATH
EOF
. ~/.bashrc