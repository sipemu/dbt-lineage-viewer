#!/usr/bin/env bash
# Logging helpers with the names and behaviour of Atlassian's
# bitbucket-pipes-toolkit common.sh (info, success, warning, fail, debug,
# enable_debug), so the pipe reads like the official ones.

gray="\\e[37m"
blue="\\e[36m"
red="\\e[31m"
yellow="\\e[33m"
green="\\e[32m"
reset="\\e[0m"

info() { echo -e "${blue}INFO: $*${reset}"; }
warning() { echo -e "${yellow}WARNING: $*${reset}"; }
success() { echo -e "${green}✔ $*${reset}"; }
fail() {
  echo -e "${red}✖ $*${reset}"
  exit 1
}

debug() {
  if [[ "${DEBUG:-false}" == "true" ]]; then
    echo -e "${gray}DEBUG: $*${reset}"
  fi
}

enable_debug() {
  if [[ "${DEBUG:-false}" == "true" ]]; then
    info "Enabling debug mode."
    set -x
  fi
}
