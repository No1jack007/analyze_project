#!/bin/bash

SCRIPT_LOCATION='/opt/application/analyze/analyze_project'

${SCRIPT_LOCATION}/mysql-backup.sh
${SCRIPT_LOCATION}/submit.sh