# Notebook data redirector

This is an AWS application that redirects requests to files publicly hosted on Box.

## Deployment

For convenience, we've included scripts for deploying the application and creating the Box webhook. The first step is to create an AWS Secrets Manager secret using the `create_secret.py` script.

```console
$ ./scripts/create_secret.py --secret-name your-secret-name --box-client-id xxx --box-client-secret xxx --box-enterprise-id xxx --box-jwt-key-id xxx --box-rsa-private-key-passphrase xxx --box-webhook-signature-key xxx --box-rsa-private-key-path /path/to/your/private-key
Created Secrets Manager secret with ARN: arn:aws:secretsmanager:xxx
```

The script will confirm that the credentials work, create a Secrets Manager secret, then output the resulting ARN.

Next, deploy the application as a CloudFormation stack with the `deploy.py` script:

```console
$ ./scripts/deploy.py --stack-name your-stack-name --box-folder-id xxx --deploy-bucket your-deploy-bucket --secret-arn arn:aws:secretsmanager:xxx
...
```

Finally, create the Box webhook with `install_webhook.py`:

```console
$ ./scripts/install_webhook.py --stack-name your-stack-name --secret-arn arn:aws:secretsmanager:xxx --box-folder-id xxx
Webhook created
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

## Running the unit tests

You'll need to install the project's dev dependencies:

```console
$ pip install .[dev]
```

Then, simply run:

```console
$ pytest
```

The Travis build is configured to fail if the contents of the `redirector` or `tests` directory fail flake8
or black checks, or if code coverage falls below a threshold.  You can confirm all of these by running
tox:

```console
$ tox
```
