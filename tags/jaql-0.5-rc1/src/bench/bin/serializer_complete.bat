SET JAVA="C:\Program Files\Java\jdk1.6.0_20\jre\bin\java"
SET JAVA_OPT= -server -Xms512m -Xmx512m
SET FS=local
SET TYPE=hadoop-read
SET BENCHMARK=Transition

%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark Transition --serializer perf -f %FS% -p perf > resultsfs.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark Transition --serializer jaqltemp -f %FS% -p jaqltemp >> resultsfs.csv

%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark ShortString --serializer perf -f %FS% -p perf >> resultsfs.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark ShortString --serializer jaqltemp -f %FS% -p jaqltemp >> resultsfs.csv

%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark LargeString --serializer perf -f %FS% -p perf >> resultsfs.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark LargeString --serializer jaqltemp -f %FS% -p jaqltemp >> resultsfs.csv

%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark Null --serializer perf -f %FS% -p perf >> resultsfs.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark Null --serializer jaqltemp -f %FS% -p jaqltemp >> resultsfs.csv

%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark LargeLong --serializer perf -f %FS% -p perf >> resultsfs.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark LargeLong --serializer jaqltemp -f %FS% -p jaqltemp >> resultsfs.csv

%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark Long --serializer perf -f %FS% -p perf >> resultsfs.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark Long --serializer jaqltemp -f %FS% -p jaqltemp >> resultsfs.csv


SET TYPE=raw-read

%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark Transition --serializer perf -f %FS% -p perf >> resultsfs.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark Transition --serializer jaqltemp -f %FS% -p jaqltemp >> resultsfs.csv

%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark ShortString --serializer perf -f %FS% -p perf >> resultsfs.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark ShortString --serializer jaqltemp -f %FS% -p jaqltemp >> resultsfs.csv

%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark LargeString --serializer perf -f %FS% -p perf >> resultsfs.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark LargeString --serializer jaqltemp -f %FS% -p jaqltemp >> resultsfs.csv

%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark Null --serializer perf -f %FS% -p perf >> resultsfs.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark Null --serializer jaqltemp -f %FS% -p jaqltemp >> resultsfs.csv

%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark LargeLong --serializer perf -f %FS% -p perf >> resultsfs.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark LargeLong --serializer jaqltemp -f %FS% -p jaqltemp >> resultsfs.csv

%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark Long --serializer perf -f %FS% -p perf >> resultsfs.csv
%JAVA% %JAVA_OPT% -jar bench.jar --type %TYPE% --benchmark Long --serializer jaqltemp -f %FS% -p jaqltemp >> resultsfs.csv
PAUSE