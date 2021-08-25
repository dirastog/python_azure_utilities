from io import BytesIO

from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient


class ADLSInterface:
    '''
    ADLSInterface - Interface to the ADLS SDK for easier
    credential handling and download/upload of files to ADLS

    :params:
        sa_name: The name of the storage account to use
        sa_key: The key for the storage account to use
    '''
    def __init__(self, sa_name, spn_credentials):
        self.storage_account_name = sa_name
        self.spn_credentials = spn_credentials
        self.service_client = self._get_service_client()

    def __get_credential(self):
        return ClientSecretCredential(
            self.spn_credentials['tenant_id'],
            self.spn_credentials['spn_id'],
            self.spn_credentials['spn_password']
        )

    def _get_service_client(self):
        credential = self.__get_credential()
        url = f'https://{self.storage_account_name}.dfs.core.windows.net/'
        return DataLakeServiceClient(
            account_url=url,
            credential=credential
        )

    def _get_file_client(self, path: str, file_system):
        return self.service_client.get_file_client(file_system, path)

    def get_file(self, remotepath: str, file_system):
        '''
        Open a file client to interact with the file and read the file directly
        '''
        file_client = self._get_file_client(
            remotepath,
            file_system=file_system
        )
        buffer = BytesIO()
        file_client.download_file().readinto(buffer)
        return buffer
