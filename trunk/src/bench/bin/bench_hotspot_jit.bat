SET JAVA="C:\Program Files\Java\jdk1.6.0_20\jre\bin\java"
SET JAVA_OPT=-Xms512m -Xmx512m -XX:+PrintCompilation -XX:+PrintGCDetails
SET BENCHMARK=Project
SET FS=

echo "NO JIT (JAVA)"
echo "========"
%JAVA% -Xint %JAVA_OPT% -jar bench.jar --type java --benchmark %BENCHMARK% %FS% > analysis_interpreter_java.txt

echo "CLIENT JVM (JAVA)"
echo "========"
%JAVA% -client %JAVA_OPT% -jar bench.jar --type java --benchmark %BENCHMARK% %FS% > analysis_client_java.txt

echo "SERVER JVM (JAVA)"
echo "========"
%JAVA% -server %JAVA_OPT% -jar bench.jar --type java --benchmark %BENCHMARK% %FS% > analysis_server_java.txt

echo "NO JIT (JAQL)"
echo "========"
%JAVA% -Xint %JAVA_OPT% -jar bench.jar --type jaql --benchmark %BENCHMARK% %FS% > analysis_interpreter_jaql.txt

echo "CLIENT JVM (JAQL)"
echo "========"
%JAVA% -client %JAVA_OPT% -jar bench.jar --type jaql --benchmark %BENCHMARK% %FS% > analysis_client_jaql.txt

echo "SERVER JVM (JAQL)"
echo "========"
%JAVA% -server %JAVA_OPT% -jar bench.jar --type jaql --benchmark %BENCHMARK% %FS% > analysis_server_jaql.txt

PAUSE