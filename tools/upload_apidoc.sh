#!/bin/bash

# Based on https://github.com/tarantool/tarantool-c/blob/463a244e7cbee1ec4b7a9682fa46705cbf5a49f2/documentation.sh

set -exuo pipefail  # Strict shell

SOURCE_BRANCH="master"
TARGET_BRANCH="gh-pages"
OUTPUT_PATH="$TRAVIS_BUILD_DIR/doc/apidoc"
COMMIT_AUTHOR_NAME="Travis CI"
COMMIT_AUTHOR_EMAIL="build@tarantool.org"
DEPLOY_KEY_ENC=".travis/deploy_apidoc.enc"

function do_compile {
    make apidoc
}

# 'git checkout --orphan' leaves a new branch w/o a commit, the next one will
# be the first
function has_commit {
    git rev-parse --verify --quiet HEAD
    return $?
}

# 'git diff --quiet' replacement with 'Last update' changes ignoring
function no_modified {
    set +x
    tmpdir=$(mktemp -d)
    echo -e '#!/bin/sh\n\ndiff -r -U 1 -I "Last updated" "$@"\nexit 0' \
        > "$tmpdir/diff_cmd.sh"
    chmod a+x "$tmpdir/diff_cmd.sh"
    res="$(git difftool --dir-diff --extcmd "$tmpdir/diff_cmd.sh" 2>/dev/null)"
    rm "$tmpdir/diff_cmd.sh"
    rmdir $tmpdir
    [ -z "$res" ]
    rc=$?
    set -x
    return $rc
}

function no_untracked {
    set +x
    res="$(git ls-files -o --exclude-standard)"
    [ -z "$res" ]
    rc=$?
    set -x
    return $rc
}

# Pull requests and commits to other branches shouldn't try to deploy, just
# build to verify
if [ "$TRAVIS_PULL_REQUEST" != "false" ] || \
        [ "$TRAVIS_BRANCH" != "$SOURCE_BRANCH" ] || \
        [ "$TRAVIS_EVENT_TYPE" != "push" ]; then
    echo "upload_apidoc.sh: Skipping deploy; just doing a build."
    do_compile
    exit 0
fi

# Save some useful information
REPO=$(git config remote.origin.url)
SSH_REPO=${REPO/https:\/\/github.com\//git@github.com:}
MSG="$(git log --oneline --no-decorate -1)"

# Clone the existing gh-pages for this repo into $OUTPUT_PATH
# Create a new empty branch if gh-pages doesn't exist yet (should only happen
# on first deploy)
git clone $REPO $OUTPUT_PATH
cd $OUTPUT_PATH
if ! git checkout $TARGET_BRANCH; then
    git checkout --orphan $TARGET_BRANCH
    git rm -rf .
fi
cd $TRAVIS_BUILD_DIR

# Clean out existing contents
tmpdir=$(mktemp -d)
mv $OUTPUT_PATH/.git $tmpdir/.git
rm -rf $OUTPUT_PATH && mkdir $OUTPUT_PATH
mv $tmpdir/.git $OUTPUT_PATH/.git
rmdir $tmpdir

# Run our compile script
do_compile

# Now let's go have some fun with the cloned repo
cd $OUTPUT_PATH
git config user.name "$COMMIT_AUTHOR_NAME"
git config user.email "$COMMIT_AUTHOR_EMAIL"

# If there are no changes to the compiled out (e.g. this is a README update)
# then just bail. Commit unconditionally if there are no commits on the branch
# (after git checkout --orphan).
if has_commit && no_modified && no_untracked; then
    echo "upload_apidoc.sh: No changes to the output on this push; exiting."
    exit 0
fi

# Commit the "changes", i.e. the new version.
# The delta will show diffs between new and old versions.
git add --all .
git status
git commit -m "apidoc build: ${MSG}"

# Get the deploy key by using Travis's stored variables to decrypt deploy_apidoc.enc
ENCRYPTED_KEY_VAR="encrypted_${ENCRYPTION_LABEL}_key"
ENCRYPTED_IV_VAR="encrypted_${ENCRYPTION_LABEL}_iv"
ENCRYPTED_KEY=${!ENCRYPTED_KEY_VAR}
ENCRYPTED_IV=${!ENCRYPTED_IV_VAR}
DEPLOY_KEY="${DEPLOY_KEY_ENC%.enc}"
ENCRYPTED_KEY_PATH="${TRAVIS_BUILD_DIR}/${DEPLOY_KEY_ENC}"
DECRYPTED_KEY_PATH="${TRAVIS_BUILD_DIR}/${DEPLOY_KEY}"
openssl aes-256-cbc -K $ENCRYPTED_KEY -iv $ENCRYPTED_IV -in "$ENCRYPTED_KEY_PATH" -out "$DECRYPTED_KEY_PATH" -d
chmod 600 "${DECRYPTED_KEY_PATH}"
eval $(ssh-agent -s)
ssh-add "${DECRYPTED_KEY_PATH}"

# Now that we're all set up, we can push.
git push $SSH_REPO $TARGET_BRANCH
