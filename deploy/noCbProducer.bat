echo The current directory is %CD%
cd %CD%
java -cp corebos-1.0-SNAPSHOT-jar-with-dependencies.jar OpenProducer simple_topic my_key my_message
pause >nul