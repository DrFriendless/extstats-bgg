cd cdk
set -a
source ../.env
set +a
export COMPONENT=api

cdk deploy --profile drfriendless --require-approval never
success=$?
if [ $success -eq 0 ]; then
    echo CDK deploy succeeded
    cd lib
    pwd
    npx ts-node --prefer-ts-exts ./post-stack.mts
else
    echo CDK deploy failed, not proceeding.
    exit 3
fi
date