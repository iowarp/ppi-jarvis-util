#!/bin/bash
# git remote add iowarp https://github.com/iowarp/ppi-jarvis-util.git
# git remote add grc https://github.com/grc-iit/jarvis-util.git
gh pr create --title $1 --body "" --repo=grc-iit/jarvis-util
gh pr create --title $1 --body "" --repo=iowarp/ppi-jarvis-util
