import json
from unittest.mock import patch

#import pytest
# from rdmt_spire.app import handler
# from rdmt_spire.constants.codes import StatusCodes
from ..app import handler
from ..constants.codes import StatusCodes
from ..constants.lambdas import FunctionTypes, MessageKeys

#TODO: add mock s3 handler / mock SQS message

class LambdaContextMock():
    # Simulates the lambda function context that is needed to get the account ID
    def __init__(self, function_name='test-function'):
        self.invoked_function_arn = f"arn:aws:lambda:us-east-1:123456789012:function:{function_name}"

def test_lambda_app():
    """
    Testing lambda handler
    """
    context = LambdaContextMock()
    base_message = {
        'archiveBucket':        'test_bucket',
        'archiveObjectKey':     'somefile.txt',
        'checksum':             'test checksum (hex)', #32 char MD5 hex
        'reprocessingState':    'none', # 'none', 'reprocessed', or 'data_release'
        'reprocessingId':       'for_testing', #string set by HLPP operators
        'externalSource':       '', 
        'filename':             'somefile.txt',
        'fileType':             'science_wfi_level_2', 
        'fileSubType':          'image', # optional and still unclear what this will actually be in ops
        'fileSize':             '100000000', # in bytes 
        'fileCreationTimestamp':'2023-10-05T14:48:00.000Z', # in UTC
        'program_category':     'CCS',
        'program_subcategory':  'HLWA',
    }

    event = {
        'Records': [{
            'body': json.dumps({'Message': json.dumps(base_message)}),
            'attributes': {'SentTimestamp': '1696517280000'},
        }]
    }
    response = handler(event, context)
    assert response['statusCode'] == StatusCodes.INVALID_FILE_SUFFIX
    assert 'error_message' in response['body'][0]

    failure_response = {'statusCode': StatusCodes.FAILURE, 'body': [{'error_message': 'expected failure'}]}
    monitor_message = {
        MessageKeys.FUNCTION_TYPE: FunctionTypes.MONITOR,
        MessageKeys.MONITOR_NAME: 'non_existing_monitor',
    }
    event = {'Records': [{'body': json.dumps(monitor_message)}]}
    with patch('rdmt_spire.app.monitor_function', return_value=failure_response) as monitor_handler:
        assert handler(event, context) == failure_response
        monitor_handler.assert_called_once_with(monitor_message)

    report_event = {MessageKeys.FUNCTION_TYPE: FunctionTypes.REPORT_GEN}
    with patch('rdmt_spire.app.report_function', return_value=failure_response) as report_handler:
        assert handler(report_event, context) == failure_response
        report_handler.assert_called_once_with(None)

    metadata_event = {MessageKeys.FUNCTION_TYPE: FunctionTypes.METADATA_CHECK}
    with patch('rdmt_spire.app.metadata_check_function', return_value=failure_response) as metadata_handler:
        assert handler(metadata_event, context) == failure_response
        metadata_handler.assert_called_once_with(metadata_event, '123456789012')
