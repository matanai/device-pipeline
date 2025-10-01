import { App } from 'aws-cdk-lib';
import { Template, Match } from 'aws-cdk-lib/assertions';
import { DevicePipelineStack } from '../lib/device-pipeline-stack';

test('creates the three app lambdas', () => {
    const app = new App();
    const stack = new DevicePipelineStack(app, 'TestStack');
    const template = Template.fromStack(stack);

    // Verify each app lambda by handler
    template.hasResourceProperties('AWS::Lambda::Function', {
        Handler: 'ingest_lambda.handler',
        Runtime: Match.stringLikeRegexp('python'),
    });
    template.hasResourceProperties('AWS::Lambda::Function', {
        Handler: 'process_lambda.handler',
        Runtime: Match.stringLikeRegexp('python'),
    });
    template.hasResourceProperties('AWS::Lambda::Function', {
        Handler: 'query_lambda.handler',
        Runtime: Match.stringLikeRegexp('python'),
    });
});
