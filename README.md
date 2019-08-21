# Notebook data redirector

This is an AWS application that redirects requests to files publicly hosted on Box.

## Deployment

To deploy the redirector, first install and configure the [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html) and [Docker](https://docs.docker.com/install/).

Next, run the following commands:

```console
$ sam build
$ sam package --s3-bucket your-s3-bucket --output-template packaged.yaml
$ sam deploy --template-file packaged.yaml --capabilities CAPABILITY_IAM --stack-name your-cloud-formation-stack
```
## Local testing

The SAM CLI and sample events make it easy to test the webhook function locally:

```console
$ sam build
$ sam local invoke "BoxWebhookFunction" -e your-sample-event.json
```

Note that this will interact with the live DynamoDB table, so proceed with caution.
