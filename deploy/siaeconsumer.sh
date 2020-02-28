#!/bin/sh
cd /home/deploy
/usr/bin/java -cp corebos-1.0-SNAPSHOT-jar-with-dependencies.jar SiaeConsumerExe
echo $? >> /home/deploy/execcode