import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List

from .constants.codes import StatusCodes
from .constants.lambdas import FunctionTypes, MessageKeys
from .lambdas.ingest import ingest_batch, ingest_single
from .lambdas.metadata_check import metadata_check_function
from .lambdas.monitor import monitor_function
from .lambdas.report import report_function

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event: Dict, context: Dict) -> Dict[str, int| List[Dict[Any, Any]]]:
    """Main runner function that is executed within each Lambda function on AWS.

    Parameters
    ----------
    event : Dict
        The notification/event that triggered the Lambda function to execute.
        For this tool, it will be from EventBridge or an SQS queue we are developing.
    context : Dict
        Contains information about the execution including things like the lambda function's name,
        version, invoking function's ARN, invoking account id, etc.

    Returns
    -------
    A JSON dictionary containing a status code and some body information depending on what is needed.
    """
    aws_account_id = context.invoked_function_arn.split(":")[4]
    # Parse the event to determine which function to call based on the message type.
    # SQS events will have a 'Records' key. These will trigger the ingest and monitor functions
    if 'Records' in event:
        # batch_size must be 1 for most execution options
        if len(event['Records']) == 1:
            body = event['Records'][0]['body']
            if isinstance(body, str):
                body = json.loads(body)

            # the SQS from DMD will not have "function_type" in it
            if MessageKeys.FUNCTION_TYPE not in body.keys():
                message_dict = json.loads(body['Message'])
                unix_timestamp = float(event['Records'][0]['attributes']['SentTimestamp'])/1000.0
                notification_datetime = datetime.fromtimestamp(unix_timestamp, timezone.utc)
                return ingest_single(message_dict, notification_datetime, aws_account_id)
            elif body[MessageKeys.FUNCTION_TYPE] == FunctionTypes.MONITOR:
                return monitor_function(body)
            else:
                raise ValueError(f"{StatusCodes.BAD_EVENT_FORMAT}: Bad function "
                                 f"type ({body[MessageKeys.FUNCTION_TYPE]}): "
                                 'Unsupported function type specified in the '
                                 'event with Records outer layer. Please check '
                                 'implementation.')
        else:
            # NOT YET FULLY IMPLEMENTED. Please ensure that the batch processing logic is correctly handled for multiple records before using.
            message_dicts = [json.loads(rec['body']['Message'])
                             for rec in event['Records']]
            function_type_checks = [MessageKeys.FUNCTION_TYPE in message_dict.keys()
                                    for message_dict in message_dicts]

            # if no function_type is specified, the 'ingest_function' should be used
            if sum(function_type_checks) == 0:

                unix_timestamps = [float(rec['attributes']['SentTimestamp'])/1000.0 for rec in event['Records']]
                notification_datetimes = [datetime.fromtimestamp(ts, timezone.utc) for ts in unix_timestamps]
                return ingest_batch(message_dicts, notification_datetimes)
            else:
                raise ValueError(f"{StatusCodes.BAD_LAMBDA_BATCH_SIZE}: A "
                                 'lambda function not running ingest_function '
                                 'received more than 1 request.')
    # Other events should have a 'function_type' key in the event dictionary. These will trigger the report and metadata_check functions
    elif MessageKeys.FUNCTION_TYPE in event.keys():
        if event[MessageKeys.FUNCTION_TYPE] == FunctionTypes.METADATA_CHECK:
            return metadata_check_function(event, aws_account_id)
        elif event[MessageKeys.FUNCTION_TYPE] == FunctionTypes.REPORT_GEN:
            report_type = event.get(MessageKeys.REPORT_TYPE)
            exp_range_start = event.get(MessageKeys.EXP_RANGE_START, None)
            exp_range_end = event.get(MessageKeys.EXP_RANGE_END, None)
            return report_function(report_type, exp_range_start, exp_range_end)
        else:
            raise ValueError(f"{StatusCodes.BAD_EVENT_FORMAT}: Unsupported "
                             'function type specified in the event '
                             f"({event[MessageKeys.FUNCTION_TYPE]}).")
    else:
        logger.error(
            "Invalid Lambda event (request_id=%s, has_records=%s, has_function_type=%s (%s))",
            getattr(context, "aws_request_id", "unknown"),
            "Records" in event,
            MessageKeys.FUNCTION_TYPE in event,
            getattr(event, MessageKeys.FUNCTION_TYPE, "None"),
        )
        raise ValueError(f"{StatusCodes.BAD_EVENT_FORMAT}: Incorrect event format provided to app.py")
