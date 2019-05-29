#!/usr/bin/env bash

#
# Checks that all Scala files that were changed and staged since the last commit
# to `master` are properly formatted with scalafmt.
#
# Usage: ./scalafmt_check_changed.sh
#

set -euf

function main {
    local scalafmt_outf=$(tempfile -p scalafmt)
    local gitdiff_outf=$(tempfile -p scalafmt)

    trap "rm -f -- '$scalafmt_outf' '$gitdiff_outf'" EXIT

    echo "Checking files with scalafmt. This might take a while."
    sbt scalafmtCheckAll | awk '/formatted properly/{ print $2 }' > $scalafmt_outf || true
    git diff --cached --name-only master | grep -E '*.scala$' > $gitdiff_outf || true

    local files_to_reformat=$(grep -F -f $gitdiff_outf $scalafmt_outf)
    if [[ -z "$files_to_reformat" ]]; then
        echo "All changed files are properly formatted. Good job!"
    else
        echo "The following files need to be reformatted:"
        echo "$files_to_reformat"
        exit 1
    fi
}

main
