from io import BytesIO, StringIO
import json

from azure.storage.blob import BlobServiceClient


class BlobConnection:
    '''
    This Class is used to make a connection to the Storage Account using the
    connecton string(Full or SAS URI connection string).
    '''
    def __init__(self, blob_conn_str: str):
        self.blob_conn_str = blob_conn_str
        self.blob_service_client = self._get_blob_service_client()

    def _get_blob_service_client(self):
        '''
        This method is used to return a BlobServiceClient Object, to enable
        operations on blob
        '''
        return BlobServiceClient.from_connection_string(self.blob_conn_str)

    def _get_blob_client(self, container: str, blob_path: str):
        '''
        This method is used to make a blob client for a particular blob
        referred by the blob_path in the container
        '''
        return self.blob_service_client.get_blob_client(
            container=container, blob=blob_path)

    def _get_container_client(self, container: str):
        return self.blob_service_client.get_container_client(
            container=container)

    def get_file(self, container: str, blob_path: str):
        '''
        This method downloads and retuns the blob specified by blob_path
        in the container 'container' as StorageStreamDownloader Object.
        '''
        blob_client = self._get_blob_client(container, blob_path)
        data = blob_client.download_blob()
        return data

    def get_json_object(self, container: str, blob_path: str):
        '''
        This method read the StorageStreamDownloader Object as a Stream
        and loads it into a BytesIO buffer, which is then loaded as a json
        object.
        '''
        data = self.get_file(container, blob_path)
        buffer = BytesIO()
        data.readinto(buffer)
        buffer.seek(0)
        return json.load(buffer)

    def save_file(self, data: bytes, container: str, blob_path: str):
        '''
        This method is used to save the bytes data as a blob in the container.
        '''
        blob_client = self._get_blob_client(container, blob_path)
        blob_client.upload_blob(data, overwrite=True)

    def save_json(self, json_object: object, container: str, blob_path: str):
        '''
        This method is used to save/overwrite the json file as a blob in the
        container
        '''
        data = json.dumps(json_object, indent=2).encode('utf-8')
        self.save_file(data, container, blob_path)

    def save_df_to_csv(self, dataframe, container: str, blob_path: str):
        '''
        This method is used to save a dataframe to blob as .csv file
        '''
        buffer = StringIO()
        dataframe.to_csv(buffer, index=False, header=True)
        self.save_file(buffer.getvalue(), container, blob_path)

    def list_files(self, container: str, directory_name=None):
        container_client = self._get_container_client(container)
        if directory_name is None:
            blob_list = container_client.list_blobs()
        else:
            blob_list = container_client.list_blobs(
                name_starts_with=directory_name+'/'
            )
        return blob_list

    def delete_blobs(self, container: str, blob_path: str):
        blob_client = self._get_blob_client(container, blob_path)
        blob_client.delete_blob(delete_snapshots='include')
