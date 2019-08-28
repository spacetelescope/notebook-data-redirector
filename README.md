# Notebook data redirector

This is an AWS application that redirects requests to files publicly hosted on Box.

## Deployment

For convenience, we've included scripts for deploying the application and creating the Box webhook. The first step is to run the `configure.py` script, which uploads your Box credentials to Secrets Manager and generates a .json file used by the other scripts.

```console
$ ./configure.py --config config.json
```

The script will ask you a series of questions.  You'll need to have your Box credentials on hand.

Next, deploy the application as a CloudFormation stack with the `deploy.py` script:

```console
$ ./deploy.py --config config.json
```

Finally, create the Box webhook with `install_webhook.py`:

```console
$ ./install_webhook.py --config config.json
```

At this point your redirector should be ready to go!

### Teardown

If you wish to remove the application, simply delete the relevant stack in the CloudFormation console.  That will remove all AWS resources except the Secrets Manager secret and the S3 bucket used to stage the Lambda archive.

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
    "BoxWebhookFunction": {
        "SECRET_ARN": "your-secret-arn",
        "BOX_FOLDER_ID": "your-box-folder-id",
        "MANIFEST_TABLE_NAME": "your-ddb-table-name"
    }
}
```

Note that this will interact the Box API and whatever DynamoDB table you specify, so proceed with caution.
