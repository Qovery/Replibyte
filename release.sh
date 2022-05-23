#!/usr/bin/env bash

set -e

TOML_FILES="\
replibyte/Cargo.toml \
subset/Cargo.toml \
dump-parser/Cargo.toml
"

old=$1
new=$2

if [ -z "${old}" ] || [ -z "${new}" ]
then
    echo "please run: $0 <old version> <new version>"
    exit 1
fi

if [ "$(git status --porcelain=v1 2>/dev/null | wc -l)" -ne 0 ]
then
    git status
    echo "There are unsaved changes in the repository, press CTRL-C to abort now or return to continue."
    read -r answer
fi

echo -n "Release process starting from '${old}' -> '${new}', do you want to continue? [y/N] "
read -r answer


case "${answer}" in
    Y*|y*)
        ;;
    *)
        echo "Aborting"
        exit 0
        ;;
esac;

echo "==> ${answer}"

echo -n "Updating TOML files:"
for toml in ${TOML_FILES}
do
    echo -n " ${toml}"
    sed -e "s/^version = \"${old}\"$/version = \"${new}\"/" -i.release "${toml}"
done
echo "."

echo "Please review the following changes. (return to continue)"
read -r answer

git diff

echo "Do you want to Continue or Rollback? [c/R]"
read -r answer

case "${answer}" in
    C*|c*)
        git checkout -b "release-v${new}"
        git commit -sa -m "Release v${new}"
        git push --set-upstream origin "release-v${new}"
        ;;
    *)
        git checkout .
        exit
        ;;
esac;

echo "Please open the following pull request we'll wait here continue when it is merged."
echo
echo "  >> https://github.com/qovery/replibyte/pull/new/release-v${new} <<"
echo
echo "Once you continue we'll generate and push the release tag with the latest 'main'"
echo
echo "WARNING: Review and wait until the pull request is merged before continuing to create the release"
read -r answer

echo "Generating release tag v${new}"

git checkout main
git pull

# The version is correctly updated in the replibyte crate cargo.toml (aka the PR is merged)
if grep -q  "version = \"${new}\"" ${TOML_FILES[0]}; then
    git tag -a -m"Release v${new}" "v${new}"
    git push --tags

    echo "Congrats release v${new} is done!"
else
    echo
    echo "It seems the version is not updated, are you sure you have merged the pull request as stated before?"
    echo "If that's not the case, you're invited to run again the release script and wait for the PR is merged before continuing."
    echo
    echo "Rollback changes"

    git branch -d "release-v${new}"
    git push origin --delete "release-v${new}"
fi
