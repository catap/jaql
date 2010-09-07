SET JAVA="C:\Program Files\Java\jdk1.6.0_20\jre\bin\java"
SET JAVA_OPT= -server -Xms512m -Xmx512m -XX:+PrintCompilation -XX:+PrintGCDetails
REM SET JAVA_OPT= -server -Xms512m -Xmx512m -XX:+PrintGCDetails -XX:+UnlockDiagnosticVMOptions -XX:+LogCompilation
SET BENCHMARK=Project
REM SET FS=-f memory
SET FS=

echo "JAVA RUN"
echo "========"
%JAVA% %JAVA_OPT% -jar bench.jar --type java --benchmark %BENCHMARK% %FS% > analysis_java.txt

echo "JSON RUN"
echo "========"
%JAVA% %JAVA_OPT% -jar bench.jar --type json --benchmark %BENCHMARK% %FS% > analysis_json.txt

echo "JAQL RUN"
echo "========"
%JAVA% %JAVA_OPT% -jar bench.jar --type jaql --benchmark %BENCHMARK% %FS% > analysis_jaql.txt


PAUSE