#!/bin/sh
# Remove the directories after running "ant test" in Jaql development
# environment
for f in `svn st | grep ^\? | sed 's/\\\\/\\//g'` 
do
  rm -fr "$f"
done
