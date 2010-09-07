SET JAVA="C:\Program Files\Java\jdk1.6.0_20\jre\bin\java"
SET JAVA_OPT= -server -Xms512m -Xmx512m
REM SET FS=-f memory
SET FS=
SET TYPE=hadoop-read
SET BENCHMARK=Transition

%JAVA% %JAVA_OPT% -jar bench.jar --type java --benchmark ArrayAverage %FS% > results_int.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type json --benchmark ArrayAverage %FS% >> results_int.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type jaql --benchmark ArrayAverage %FS% >> results_int.csv

%JAVA% %JAVA_OPT% -jar bench.jar --type java --benchmark FieldAccess %FS% >> results_int.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type json --benchmark FieldAccess %FS% >> results_int.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type jaql --benchmark FieldAccess %FS% >> results_int.csv

%JAVA% %JAVA_OPT% -jar bench.jar --type java --benchmark KeyExtract %FS% >> results_int.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type json --benchmark KeyExtract %FS% >> results_int.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type jaql --benchmark KeyExtract %FS% >> results_int.csv

%JAVA% %JAVA_OPT% -jar bench.jar --type java --benchmark Project %FS% >> results_int.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type json --benchmark Project %FS% >> results_int.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type jaql --benchmark Project %FS% >> results_int.csv

%JAVA% %JAVA_OPT% -jar bench.jar --type java --benchmark ProjectArray %FS% >> results_int.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type json --benchmark ProjectArray %FS% >> results_int.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type jaql --benchmark ProjectArray %FS% >> results_int.csv

%JAVA% %JAVA_OPT% -jar bench.jar --type java --benchmark StringConcat %FS% >> results_int.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type json --benchmark StringConcat %FS% >> results_int.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type jaql --benchmark StringConcat %FS% >> results_int.csv

%JAVA% %JAVA_OPT% -jar bench.jar --type java --benchmark Transform %FS% >> results_int.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type json --benchmark Transform %FS% >> results_int.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type jaql --benchmark Transform %FS% >> results_int.csv

PAUSE