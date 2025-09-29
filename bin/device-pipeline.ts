#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { DevicePipelineStack } from '../lib/device-pipeline-stack';

const app = new cdk.App();
new DevicePipelineStack(app, 'DevicePipelineStack', {
  env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION },
});