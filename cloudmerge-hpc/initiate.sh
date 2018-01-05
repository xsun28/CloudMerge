#!/bin/bash
#starcluster createvolume --name=cloudmerge_hpc --shutdown-volume-host 500 us-east-1a
starcluster start mpicluster
#starcluster put mpicluster bootstrap.sh /home/ubuntu
#starcluster sshmaster mpicluster '/home/ubuntu/bootstrap.sh'
