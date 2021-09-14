import json
from datetime import datetime

from azure.servicebus.control_client import (
    ServiceBusService, Message
)

from utility_package.utils.logging_utility import get_logger

LOGGER = get_logger(__name__)


class ServiceBusSASTokenAuthentication:
    '''
    class  ServiceBusSASTokenAuthentication override
    '''
    def __init__(self, sas_token):
        self.sas_token = sas_token

    def sign_request(self, request, httpclient):  # pylint: disable=W0613;
        # Function Override, httpclient needed, though not used
        request.headers.append(
            ('Authorization', self._get_authorization())
        )

    def _get_authorization(self):
        return self.sas_token


class ServiceBusUtility:
    def __init__(self, service_namespace, queue_name, sas_token):
        self.sas_token = sas_token
        self.service_namespace = service_namespace
        self.queue_name = queue_name

        self.service_bus_client = self._get_service_bus_client()

    def _get_service_bus_client(self):
        sb_client = ServiceBusService(
            authentication=ServiceBusSASTokenAuthentication(
                sas_token=self.sas_token
            ),
            service_namespace=self.service_namespace
        )
        return sb_client

    def send_message(self, message):
        LOGGER.info(
            "Sending Message to %s queue at %s",
            self.queue_name, datetime.utcnow()
        )
        message = json.dumps(message)
        msg_obj = Message(body=str(message).encode('utf-8'))
        self.service_bus_client.send_queue_message(
            self.queue_name, message=msg_obj
        )
