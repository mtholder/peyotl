#!/usr/bin/env bash

# Bash completion for opentree
_opentree()
{
    local cur prev opts
    COMPREPLY=()
    cur="${COMP_WORDS[COMP_CWORD]}"
    prev="${COMP_WORDS[COMP_CWORD-1]}"
    NUM_ARGS=${#COMP_WORDS[@]}
    if test $NUM_ARGS -lt 2 ; then
        opts=$(opentree --show-completions)
    else
        opts=$(opentree--show-completions ${COMP_WORDS[*]})
    fi
    if [[ ${cur} == * ]] ; then
        COMPREPLY=( $(compgen -W "${opts}" -- ${cur}) )
        return 0
    fi
}

complete -F _opentree opentree


