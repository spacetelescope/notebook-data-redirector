# Notebook data redirector

This is an AWS application that redirects requests to files publicly hosted on Box.

## Deployment

To deploy the redirector, first install and configure the [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/serverless-sam-cli-install.html).

Next, run the following commands:

```console
$ sam build
$ sam package --s3-bucket your-s3-bucket --output-template packaged.yaml
$ sam deploy --template-file packaged.yaml --capabilities CAPABILITY_IAM --stack-name your-cloud-formation-stack --parameter-overrides SecretArn=your-secret-arn ManifestTableName=your-ddb-table-name
```
## Local testing

The SAM CLI and sample events make it easy to test the webhook function locally.  The CLI uses Docker to run the Lambda code in a container, so you'll need to [install it](https://docs.docker.com/install/).

```console
$ sam build
$ sam local invoke "BoxWebhookFunction" -e your-sample-event.json -n env.json
```

Where `your-sample-event.json` contains a Box webhook event serialized to JSON, and `env.json` contains
this:

```json
{
    "BoxWebhookFunction": { "MANIFEST_TABLE_NAME": "your-ddb-table-name" }
}
```

Note that this will interact the Box API and whatever DynamoDB table you specify, so proceed with caution.
