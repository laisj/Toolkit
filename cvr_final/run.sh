#!/bin/sh

git add *.sh
git add ./src
git add ./conf
git add ./pom*
git add ./scripts
git add ./tensorflow*
git add ./ensemble
git commit -m "add code"
git push

