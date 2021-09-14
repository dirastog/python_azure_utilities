from avro import schema
from azure.eventhub import EventData
from azure.eventhub.aio import EventHubConsumerClient, EventHubProducerClient
from azure.eventhub.extensions.checkpointstoreblobaio import (
    BlobCheckpointStore)
from azure.identity import ClientSecretCredential
from azure.schemaregistry import SchemaRegistryClient
from pyspark.sql.types import _parse_datatype_json_string

from utility_package.utils.keyvault_utility import KeyvaultSecretsUtility


class EventHubConnection:
    '''
    This class is used to create a connection to schema registry
    and perform retrieval and registering of schema from and into
    registry.
    '''
    def __init__(
        self, spn_credentials, config, spark,
        consumer_group='$Default'
    ):
        '''
        The constructor instantiates an Object and creates a schema registry
        client.
        '''
        self.config = config
        self.event_hub_namespace = self.config['event_hub'][0]['namespace_name']
        self.schema_registry_client = self._get_schema_registry_client(
            spn_credentials
        )
        self.kv_client = KeyvaultSecretsUtility(
            spn_credentials,
            self.config['keyvault'][0]['name']
        )
        self.consumer_group = consumer_group
        self.checkpoint_store = self._create_checkpoint_store()
        self.consumer_client = self._get_event_hub_consumer_client()
        self.producer_client = self._get_event_hub_producer_client()
        self.spark = spark

    def _create_checkpoint_store(self):
        checkpoint_container_name = (
            self.config['event_hub'][0]['check_point_container_name']
        )
        blob_conn_str_secret = (
            self.config['blob_details'][0]['blob_conn_str_secret']
        )
        _, blob_conn_str = self.kv_client.get_secret(blob_conn_str_secret)
        checkpoint_store = (
            BlobCheckpointStore.
            from_connection_string(
                conn_str=blob_conn_str,
                container_name=checkpoint_container_name
            ))
        return checkpoint_store

    def _get_event_hub_consumer_client(self):
        conn_str_secret_name = (
            self.config['event_hub'][0]['event_hub_namespace_conn_str_secret']
        )
        consumer_group = self.consumer_group
        event_hub_name = self.config['event_hub'][0]['eventhub_name']
        _, conn_str = self.kv_client.get_secret(conn_str_secret_name)
        return EventHubConsumerClient.from_connection_string(
            conn_str=conn_str,
            consumer_group=consumer_group,
            eventhub_name=event_hub_name,
            checkpoint_store=self.checkpoint_store
        )

    def _get_event_hub_producer_client(self):
        conn_str_secret_name = (
            self.config['event_hub'][0]['event_hub_namespace_conn_str_secret']
        )
        event_hub_name = self.config['event_hub'][0]['eventhub_reject_name']
        _, conn_str = self.kv_client.get_secret(conn_str_secret_name)
        return EventHubProducerClient.from_connection_string(
            conn_str=conn_str,
            eventhub_name=event_hub_name
        )

    def _get_schema_registry_client(self, spn_credentials):
        '''
        This function creates a Schema Registry client by using spn_credentials
        (spn_id, spn_passworc, tenant_id)
        '''
        event_hub_endpoint = (
            f'{self.event_hub_namespace}.servicebus.windows.net'
        )
        credential = ClientSecretCredential(
            tenant_id=spn_credentials['tenant_id'],
            client_id=spn_credentials['spn_id'],
            client_secret=spn_credentials['spn_password']
        )
        return SchemaRegistryClient(
            endpoint=event_hub_endpoint, credential=credential
        )

    async def write_event_to_event_hub(self, event_data):
        event_data_batch = await self.producer_client.create_batch()
        event_data_batch.add(EventData(event_data))
        await self.producer_client.send_batch(event_data_batch)

    def get_json_schema(self, schema_id):
        '''
        This function retrives the schema with respect to a schema id
        and returns it as a json string
        '''
        base_schema = self.schema_registry_client.get_schema(schema_id)
        schema_content = base_schema.schema_content
        return schema_content

    def get_avro_schema(self, schema_id):
        '''
        This function retrives the schema with respect to a schema id
        and returns it as a avro schema string
        '''
        base_schema = self.schema_registry_client.get_schema(schema_id)
        avro_schema = schema.parse(base_schema.schema_content)
        return avro_schema

    def get_struct_schema_from_avro(self, schema_id):
        '''
        This function retrives the schema with respect to a schema id, parses
        it as avro schema and then converts it into struct type schema.
        '''
        base_schema = self.schema_registry_client.get_schema(schema_id)
        # print(base_schema.schema_content)
        avro_schema = self.spark._jvm.org.apache.avro.Schema.Parser().parse(
            base_schema.schema_content
        )
        converted_schema = (
            self.spark._jvm.org.apache.spark.sql.avro.
            SchemaConverters.toSqlType(avro_schema).dataType()
        )
        struct_schema = _parse_datatype_json_string(converted_schema.json())
        return struct_schema

    def get_struct_schema_from_json(self, schema_id):
        '''
        This function retrives the schema with respect to a schema id, parses
        it as json schema and then converts it into struct type schema.
        '''
        base_schema = self.schema_registry_client.get_schema(schema_id)
        struct_type_schema = (
            self.spark._jvm.org.zalando.spark.jsonschema.SchemaConverter.
            convertContent(base_schema.schema_content)
        )
        return struct_type_schema

    def register_schema_to_registry(
        self, schema_name, schema_content, schema_group, **kwargs
    ):
        '''
        This function is used to register a schema to the Schema Registry using
        the schema name and Avro Serializing type and returns the schema id
        (GUID).
        '''
        serialisation_type = kwargs.get('serialisation_type', 'Avro')
        schema_properties = self.schema_registry_client.register_schema(
            schema_group, schema_name, serialisation_type, schema_content
        )
        return schema_properties.schema_id
